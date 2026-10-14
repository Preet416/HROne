import calendar
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import os
from typing import Annotated, Any, Dict, List, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, status
from fastapi.responses import HTMLResponse
import pathlib
from pydantic import BaseModel, Field, StrictInt
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError

# Real environment variables must take precedence over .env
load_dotenv(override=False)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# Critical Requirement 1: tls=True, tlsAllowInvalidCertificates=True
client = MongoClient(
    MONGO_URI,
    tz_aware=True,
    tls=True,
    tlsAllowInvalidCertificates=True
)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))

EpochMillis = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]


def round_half_up(val: Optional[float | Decimal], decimals: int = 2) -> Optional[float]:
    if val is None:
        return None
    d = Decimal(str(val))
    target = Decimal("10") ** -decimals
    return float(d.quantize(target, rounding=ROUND_HALF_UP))


def dt_to_epoch_ms(dt_val: Optional[datetime]) -> Optional[int]:
    if dt_val is None:
        return None
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    return int(dt_val.timestamp() * 1000)


def epoch_ms_to_dt(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    # R1: Instants are truncated to whole seconds
    sec = ms // 1000
    return datetime.fromtimestamp(sec, tz=timezone.utc)


def parse_time_str(t_str: str) -> time:
    parts = t_str.split(":")
    return time(hour=int(parts[0]), minute=int(parts[1]))


def determine_attendance_date(punch_in_dt: datetime, shift_start: str, shift_end: str) -> str:
    # R1: Shift times and attendance dates are IST
    punch_ist = punch_in_dt.astimezone(IST)
    p_date = punch_ist.date()
    p_time = punch_ist.time()

    start_t = parse_time_str(shift_start)
    end_t = parse_time_str(shift_end)

    # Overnight shift: shift_end <= shift_start
    if end_t <= start_t:
        if p_time < end_t:
            return (p_date - timedelta(days=1)).isoformat()
    return p_date.isoformat()


def compute_late_minutes(punch_in_dt: datetime, att_date_str: str, shift_start: str) -> int:
    # R2: Grace period 10 min. Late only if strictly more than 10:00 after shift_start.
    att_date = date.fromisoformat(att_date_str)
    start_t = parse_time_str(shift_start)
    shift_start_dt = datetime.combine(att_date, start_t, tzinfo=IST)

    diff_sec = (punch_in_dt - shift_start_dt).total_seconds()
    if diff_sec > 600:
        return int(diff_sec // 60)
    return 0


def compute_derived_fields(
    punch_in_dt: Optional[datetime],
    punch_out_dt: Optional[datetime],
    att_date_str: str,
    shift_start: str,
    shift_end: str
) -> Dict[str, Any]:
    if punch_in_dt is None or punch_out_dt is None:
        return {
            "work_hours": None,
            "overtime_minutes": 0,
            "half_day": False
        }

    # R4: (punch_out - punch_in) in seconds / 3600, rounded 2 decimals half-up
    work_seconds = (punch_out_dt - punch_in_dt).total_seconds()
    d = Decimal(int(work_seconds)) / Decimal(3600)
    work_hrs = float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    # R5: half_day = true when rounded work_hours < 4.50
    half_day = work_hrs < 4.50

    # R3: Overtime = whole minutes between shift_end and punch_out, only if >= 30
    att_date = date.fromisoformat(att_date_str)
    start_t = parse_time_str(shift_start)
    end_t = parse_time_str(shift_end)

    if end_t <= start_t:
        shift_end_dt = datetime.combine(att_date + timedelta(days=1), end_t, tzinfo=IST)
    else:
        shift_end_dt = datetime.combine(att_date, end_t, tzinfo=IST)

    ot_sec = (punch_out_dt - shift_end_dt).total_seconds()
    ot_min = int(ot_sec // 60)
    overtime_minutes = ot_min if ot_min >= 30 else 0

    return {
        "work_hours": work_hrs,
        "overtime_minutes": overtime_minutes,
        "half_day": half_day
    }


def format_attendance_doc(doc: dict) -> dict:
    history = []
    for h in doc.get("history", []) or []:
        entry = {
            "at": dt_to_epoch_ms(h.get("at")),
            "by": h.get("by"),
            "reason": h.get("reason"),
            "changes": {}
        }
        for k, v in (h.get("changes") or {}).items():
            if k in ["punch_in", "punch_out"]:
                entry["changes"][k] = {
                    "from": dt_to_epoch_ms(v.get("from")) if v.get("from") else None,
                    "to": dt_to_epoch_ms(v.get("to")) if v.get("to") else None
                }
            else:
                entry["changes"][k] = {
                    "from": v.get("from"),
                    "to": v.get("to")
                }
        history.append(entry)

    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_epoch_ms(doc.get("punch_in")),
        "punch_out": dt_to_epoch_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes", 0),
        "overtime_minutes": doc.get("overtime_minutes", 0),
        "half_day": doc.get("half_day", False),
        "history": history
    }


def build_employee_monthly_pipeline(emp_code: str, month: str, num_days: int) -> List[Dict[str, Any]]:
    last_day_str = f"{month}-{num_days:02d}"
    return [
        {
            "$match": {
                "emp_code": emp_code,
                "date": {"$gte": f"{month}-01", "$lte": last_day_str}
            }
        },
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date"}}}, 5]}
                                ]
                            },
                            {"$cond": ["$half_day", 0.5, 1.0]},
                            0.0
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, "$late_minutes", 0]}
                },
                "total_overtime_minutes": {
                    "$sum": {"$cond": [{"$gt": ["$overtime_minutes", 0]}, "$overtime_minutes", 0]}
                }
            }
        }
    ]


def build_department_summary_pipeline(month: str, num_days: int, department: Optional[str] = None) -> List[Dict[str, Any]]:
    last_day_str = f"{month}-{num_days:02d}"
    match_stage: Dict[str, Any] = {"joined_on": {"$lte": last_day_str}}
    if department:
        match_stage["department"] = department

    return [
        {"$match": match_stage},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"emp": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$emp_code", "$$emp"]},
                                    {"$gte": ["$date", f"{month}-01"]},
                                    {"$lte": ["$date", last_day_str]}
                                ]
                            }
                        }
                    }
                ],
                "as": "logs"
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "headcount_emps": {"$addToSet": "$emp_code"},
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$logs.date"}}}, 5]}
                                ]
                            },
                            {"$cond": ["$logs.half_day", 0.5, 1.0]},
                            0.0
                        ]
                    }
                },
                "work_hours_sum": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$ne": ["$logs.work_hours", None]}
                                ]
                            },
                            "$logs.work_hours",
                            0.0
                        ]
                    }
                },
                "work_hours_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$ne": ["$logs.work_hours", None]}
                                ]
                            },
                            1,
                            0
                        ]
                    }
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$logs.late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$cond": [{"$gt": ["$logs.late_minutes", 0]}, "$logs.late_minutes", 0]}
                },
                "leave_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "LEAVE"]}, 1, 0]}
                },
                "on_duty_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "ON_DUTY"]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "department": "$_id",
                "headcount": {"$size": "$headcount_emps"},
                "present_days": 1,
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$work_hours_count", 0]},
                        {"$divide": ["$work_hours_sum", "$work_hours_count"]},
                        None
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1
            }
        },
        {"$sort": {"department": 1}}
    ]


def build_late_leaderboard_pipeline(month: str, num_days: int, department: Optional[str] = None) -> List[Dict[str, Any]]:
    last_day_str = f"{month}-{num_days:02d}"
    pipeline: List[Dict[str, Any]] = [
        {
            "$match": {
                "date": {"$gte": f"{month}-01", "$lte": last_day_str},
                "late_minutes": {"$gt": 0}
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1}
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "emp"
            }
        },
        {"$unwind": "$emp"}
    ]
    if department:
        pipeline.append({"$match": {"emp.department": department}})
    pipeline.append({"$sort": {"total_late_minutes": -1, "_id": 1}})
    return pipeline


def build_department_trend_pipeline(department: str, from_str: str, to_str: str) -> List[Dict[str, Any]]:
    from_d = date.fromisoformat(from_str)
    to_d = date.fromisoformat(to_str)
    from_dt = datetime.combine(from_d, time.min, tzinfo=timezone.utc)
    to_dt = datetime.combine(to_d, time.min, tzinfo=timezone.utc)
    to_next_dt = to_dt + timedelta(days=1)

    return [
        {
            "$match": {
                "date": {"$gte": from_str, "$lte": to_str}
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "as": "emp"
            }
        },
        {"$unwind": "$emp"},
        {"$match": {"emp.department": department}},
        {
            "$group": {
                "_id": "$date",
                "present_count": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                            {"$cond": ["$half_day", 0.5, 1.0]},
                            0.0
                        ]
                    }
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "date_dt": {"$dateFromString": {"dateString": "$_id"}},
                "present_count": 1,
                "late_count": 1
            }
        },
        {
            "$unionWith": {
                "coll": "employees",
                "pipeline": [
                    {"$match": {"department": department}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "date_dt": {"$literal": from_dt},
                            "present_count": {"$literal": 0.0},
                            "late_count": {"$literal": 0}
                        }
                    }
                ]
            }
        },
        {
            "$group": {
                "_id": "$date_dt",
                "present_count": {"$max": "$present_count"},
                "late_count": {"$max": "$late_count"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date_dt": "$_id",
                "present_count": 1,
                "late_count": 1
            }
        },
        {
            "$densify": {
                "field": "date_dt",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [from_dt, to_next_dt]
                }
            }
        },
        {
            "$addFields": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_dt"}},
                "present_count": {"$ifNull": ["$present_count", 0.0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$date_dt"}, 5]}
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"cur_date": "$date"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$department", department]},
                                    {"$lte": ["$joined_on", "$$cur_date"]}
                                ]
                            }
                        }
                    },
                    {"$count": "count"}
                ],
                "as": "hc"
            }
        },
        {
            "$addFields": {
                "headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.count", 0]}, 0]}
            }
        },
        {
            "$addFields": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$divide": ["$present_count", "$headcount"]},
                        None
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date_dt": 1},
                "output": {
                    "moving_avg_7d": {
                        "$avg": "$attendance_rate",
                        "window": {"documents": [-6, 0]}
                    }
                }
            }
        },
        {"$sort": {"date": 1}}
    ]


class EmployeeCreate(BaseModel):
    emp_code: str = Field(..., pattern=r"^EMP\d{4,6}$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(..., min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str = Field(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: Literal["PRESENT", "WFH", "ON_DUTY"] = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    reason: str = Field(..., min_length=5, max_length=200)
    regularized_by: str = Field(..., min_length=1, max_length=50)
    status: Optional[Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None


def init_indexes():
    db.employees.create_index([("emp_code", ASCENDING)], unique=True)
    db.employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)])
    db.employees.create_index([("joined_on", ASCENDING)])

    db.attendance_logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True)
    db.attendance_logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)])
    db.attendance_logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)])
    db.attendance_logs.create_index([("date", ASCENDING), ("late_minutes", ASCENDING), ("emp_code", ASCENDING)])
    db.attendance_logs.create_index([("date", ASCENDING), ("status", ASCENDING)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_indexes()
    except Exception:
        pass
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

INDEX_PATH = pathlib.Path(__file__).parent / "index.html"


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def get_dashboard():
    if INDEX_PATH.exists():
        return HTMLResponse(content=INDEX_PATH.read_text(encoding="utf-8"))
    return HTMLResponse("<h2>Dashboard index.html not found</h2>")


@app.get("/health", tags=["system"])
def health():
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except ConnectionFailure:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="database unavailable")


@app.post("/employees", status_code=status.HTTP_201_CREATED, tags=["employees"])
def create_employee(req: EmployeeCreate):
    if req.shift_start == req.shift_end:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="shift_start must differ from shift_end")

    now = datetime.now(timezone.utc).replace(microsecond=0)
    doc = {
        "emp_code": req.emp_code,
        "name": req.name,
        "email": req.email,
        "department": req.department,
        "shift_start": req.shift_start,
        "shift_end": req.shift_end,
        "joined_on": req.joined_on,
        "created_at": now
    }
    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="employee already exists")

    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_epoch_ms(now)
    }


@app.get("/employees", tags=["employees"])
def list_employees(
    department: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100)
):
    query = {}
    if department:
        query["department"] = department

    total = db.employees.count_documents(query)
    skip = (page - 1) * page_size
    cursor = db.employees.find(query).sort("emp_code", ASCENDING).skip(skip).limit(page_size)

    items = []
    for doc in cursor:
        items.append({
            "emp_code": doc["emp_code"],
            "name": doc["name"],
            "email": doc["email"],
            "department": doc["department"],
            "shift_start": doc["shift_start"],
            "shift_end": doc["shift_end"],
            "joined_on": doc["joined_on"],
            "created_at": dt_to_epoch_ms(doc.get("created_at"))
        })

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=status.HTTP_201_CREATED, tags=["attendance"])
def punch_in(req: PunchInRequest):
    emp = db.employees.find_one({"emp_code": req.emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="employee not found")

    if req.punched_at is not None:
        p_dt = epoch_ms_to_dt(req.punched_at)
    else:
        p_dt = datetime.now(timezone.utc).replace(microsecond=0)

    att_date = determine_attendance_date(p_dt, emp["shift_start"], emp["shift_end"])
    late_min = compute_late_minutes(p_dt, att_date, emp["shift_start"])

    record = {
        "emp_code": req.emp_code,
        "date": att_date,
        "status": req.status,
        "punch_in": p_dt,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late_min,
        "overtime_minutes": 0,
        "half_day": False,
        "history": []
    }

    try:
        db.attendance_logs.insert_one(record)
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="already punched in for that date")

    return format_attendance_doc(record)


@app.post("/attendance/punch-out", tags=["attendance"])
def punch_out(req: PunchOutRequest):
    emp = db.employees.find_one({"emp_code": req.emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="employee not found")

    if req.punched_at is not None:
        p_dt = epoch_ms_to_dt(req.punched_at)
    else:
        p_dt = datetime.now(timezone.utc).replace(microsecond=0)

    record = db.attendance_logs.find_one(
        {"emp_code": req.emp_code, "punch_in": {"$lte": p_dt}},
        sort=[("punch_in", DESCENDING)]
    )

    if not record or record.get("punch_in") is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="no matching punch-in found")

    if record.get("punch_out") is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="record already punched out")

    punch_in_dt = record["punch_in"]
    if punch_in_dt.tzinfo is None:
        punch_in_dt = punch_in_dt.replace(tzinfo=timezone.utc)

    diff_sec = (p_dt - punch_in_dt).total_seconds()
    if diff_sec <= 0 or diff_sec > 86400:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="punched_at must be after punch_in and at most 24 hours after it")

    derived = compute_derived_fields(punch_in_dt, p_dt, record["date"], emp["shift_start"], emp["shift_end"])

    res = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {
            "punch_out": p_dt,
            "work_hours": derived["work_hours"],
            "overtime_minutes": derived["overtime_minutes"],
            "half_day": derived["half_day"]
        }},
        return_document=ReturnDocument.AFTER
    )

    if not res:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="record already punched out")

    return format_attendance_doc(res)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    date_to: Optional[str] = Query(default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    status: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100)
):
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="date_from must not be after date_to")

    valid_statuses = ["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
    if status is not None and status not in valid_statuses:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="invalid status")

    query: Dict[str, Any] = {}
    if emp_code:
        query["emp_code"] = emp_code
    if status:
        query["status"] = status
    if date_from and date_to:
        query["date"] = {"$gte": date_from, "$lte": date_to}
    elif date_from:
        query["date"] = {"$gte": date_from}
    elif date_to:
        query["date"] = {"$lte": date_to}

    total = db.attendance_logs.count_documents(query)
    skip = (page - 1) * page_size
    cursor = db.attendance_logs.find(query).sort([("date", DESCENDING), ("emp_code", ASCENDING)]).skip(skip).limit(page_size)

    items = [format_attendance_doc(doc) for doc in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(
    emp_code: str,
    date_val: str = Path(..., alias="date", pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    req: RegularizeRequest = ...
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="employee not found")

    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": date_val})
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="record not found")

    target_status = req.status if req.status is not None else record["status"]

    if target_status in ["ABSENT", "LEAVE"]:
        if req.punch_in is not None or req.punch_out is not None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="punches not allowed when setting ABSENT or LEAVE")
        final_punch_in = None
        final_punch_out = None
        final_late_minutes = 0
        derived = {"work_hours": None, "overtime_minutes": 0, "half_day": False}
    else:
        final_punch_in = epoch_ms_to_dt(req.punch_in) if req.punch_in is not None else record.get("punch_in")
        final_punch_out = epoch_ms_to_dt(req.punch_out) if req.punch_out is not None else record.get("punch_out")

        if final_punch_in is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="presence status requires punch_in")

        computed_date = determine_attendance_date(final_punch_in, emp["shift_start"], emp["shift_end"])
        if computed_date != date_val:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="punch_in must stay on the record's attendance date")

        if final_punch_out is not None:
            if final_punch_in.tzinfo is None:
                final_punch_in = final_punch_in.replace(tzinfo=timezone.utc)
            if final_punch_out.tzinfo is None:
                final_punch_out = final_punch_out.replace(tzinfo=timezone.utc)
            diff_sec = (final_punch_out - final_punch_in).total_seconds()
            if diff_sec <= 0 or diff_sec > 86400:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="punch_out must be after punch_in and within 24 hours")

        final_late_minutes = compute_late_minutes(final_punch_in, date_val, emp["shift_start"])
        derived = compute_derived_fields(final_punch_in, final_punch_out, date_val, emp["shift_start"], emp["shift_end"])

    changes: Dict[str, Any] = {}
    if target_status != record["status"]:
        changes["status"] = {"from": record["status"], "to": target_status}

    old_pi_ms = dt_to_epoch_ms(record.get("punch_in"))
    new_pi_ms = dt_to_epoch_ms(final_punch_in)
    if old_pi_ms != new_pi_ms:
        changes["punch_in"] = {"from": record.get("punch_in"), "to": final_punch_in}

    old_po_ms = dt_to_epoch_ms(record.get("punch_out"))
    new_po_ms = dt_to_epoch_ms(final_punch_out)
    if old_po_ms != new_po_ms:
        changes["punch_out"] = {"from": record.get("punch_out"), "to": final_punch_out}

    if derived["work_hours"] != record.get("work_hours"):
        changes["work_hours"] = {"from": record.get("work_hours"), "to": derived["work_hours"]}

    if final_late_minutes != record.get("late_minutes", 0):
        changes["late_minutes"] = {"from": record.get("late_minutes", 0), "to": final_late_minutes}

    if derived["overtime_minutes"] != record.get("overtime_minutes", 0):
        changes["overtime_minutes"] = {"from": record.get("overtime_minutes", 0), "to": derived["overtime_minutes"]}

    if derived["half_day"] != record.get("half_day", False):
        changes["half_day"] = {"from": record.get("half_day", False), "to": derived["half_day"]}

    if not changes:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="no changes detected")

    now = datetime.now(timezone.utc).replace(microsecond=0)
    history_entry = {
        "at": now,
        "by": req.regularized_by,
        "reason": req.reason,
        "changes": changes
    }

    updated_fields = {
        "status": target_status,
        "punch_in": final_punch_in,
        "punch_out": final_punch_out,
        "work_hours": derived["work_hours"],
        "late_minutes": final_late_minutes,
        "overtime_minutes": derived["overtime_minutes"],
        "half_day": derived["half_day"]
    }

    res = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "history": record.get("history", [])},
        {
            "$set": updated_fields,
            "$push": {"history": history_entry}
        },
        return_document=ReturnDocument.AFTER
    )

    if not res:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="concurrent update conflict")

    return format_attendance_doc(res)


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def get_monthly_employee_analytics(
    emp_code: str,
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="employee not found")

    year, m = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m)[1]
    start_of_month = date(year, m, 1)
    end_of_month = date(year, m, num_days)
    joined_date = date.fromisoformat(emp["joined_on"])

    working_days = 0
    if joined_date <= end_of_month:
        cur = max(start_of_month, joined_date)
        while cur <= end_of_month:
            if cur.weekday() < 5:
                working_days += 1
            cur += timedelta(days=1)

    pipeline = build_employee_monthly_pipeline(emp_code, month, num_days)
    res = list(db.attendance_logs.aggregate(pipeline))

    if res:
        row = res[0]
        present_days = round_half_up(row.get("present_days", 0.0), 2)
        leave_days = row.get("leave_days", 0)
        late_count = row.get("late_count", 0)
        total_late_minutes = row.get("total_late_minutes", 0)
        total_overtime_minutes = row.get("total_overtime_minutes", 0)
    else:
        present_days = 0.0
        leave_days = 0
        late_count = 0
        total_late_minutes = 0
        total_overtime_minutes = 0

    attendance_pct = None
    if working_days > 0:
        attendance_pct = round_half_up((present_days / working_days) * 100.0, 2)

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late_minutes,
        "total_overtime_minutes": total_overtime_minutes,
        "attendance_pct": attendance_pct
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def get_department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None
):
    year, m = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m)[1]

    pipeline = build_department_summary_pipeline(month, num_days, department)
    cursor = db.employees.aggregate(pipeline)

    items = []
    for doc in cursor:
        items.append({
            "department": doc["department"],
            "headcount": doc["headcount"],
            "present_days": round_half_up(doc.get("present_days", 0.0), 2),
            "avg_work_hours": round_half_up(doc.get("avg_work_hours"), 2),
            "late_count": doc.get("late_count", 0),
            "total_late_minutes": doc.get("total_late_minutes", 0),
            "leave_count": doc.get("leave_count", 0),
            "on_duty_count": doc.get("on_duty_count", 0)
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def get_late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(default=10, ge=1, le=50),
    department: Optional[str] = None
):
    year, m = map(int, month.split("-"))
    num_days = calendar.monthrange(year, m)[1]

    pipeline = build_late_leaderboard_pipeline(month, num_days, department)
    docs = list(db.attendance_logs.aggregate(pipeline))

    ranked_items = []
    current_rank = 1
    for i, doc in enumerate(docs):
        if i > 0 and doc["total_late_minutes"] < docs[i - 1]["total_late_minutes"]:
            current_rank = i + 1

        if current_rank > limit:
            break

        ranked_items.append({
            "rank": current_rank,
            "emp_code": doc["_id"],
            "name": doc["emp"]["name"],
            "department": doc["emp"]["department"],
            "total_late_minutes": doc["total_late_minutes"],
            "late_count": doc["late_count"]
        })

    return {"month": month, "items": ranked_items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def get_department_trend(
    department: str,
    from_date: str = Query(..., alias="from", pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    to_date: str = Query(..., alias="to", pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
):
    if db.employees.count_documents({"department": department}) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="department not found")

    from_d = date.fromisoformat(from_date)
    to_d = date.fromisoformat(to_date)
    if to_d < from_d:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="to must be greater than or equal to from")
    if (to_d - from_d).days + 1 > 92:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="date range exceeds 92 days")

    pipeline = build_department_trend_pipeline(department, from_date, to_date)
    results = list(db.attendance_logs.aggregate(pipeline))

    items = []
    for doc in results:
        items.append({
            "date": doc["date"],
            "is_working_day": doc["is_working_day"],
            "headcount": doc["headcount"],
            "present_count": round_half_up(doc.get("present_count", 0.0), 2),
            "late_count": doc.get("late_count", 0),
            "attendance_rate": round_half_up(doc.get("attendance_rate"), 4),
            "moving_avg_7d": round_half_up(doc.get("moving_avg_7d"), 4)
        })

    return {"department": department, "items": items}


@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: str = Path(..., pattern=r"^(attendance_list|employee_monthly|department_summary|late_leaderboard|department_trend)$"),
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
    limit: int = Query(default=10, ge=1, le=50),
    date_from: Optional[str] = Query(default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    date_to: Optional[str] = Query(default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    status_val: Optional[str] = Query(default=None, alias="status"),
    from_val: Optional[str] = Query(default=None, alias="from", pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    to_val: Optional[str] = Query(default=None, alias="to", pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100)
):
    if endpoint == "attendance_list":
        if date_from and date_to and date_from > date_to:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="date_from must not be after date_to")
        valid_statuses = ["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
        if status_val is not None and status_val not in valid_statuses:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="invalid status")

        query: Dict[str, Any] = {}
        if emp_code:
            query["emp_code"] = emp_code
        if status_val:
            query["status"] = status_val
        if date_from and date_to:
            query["date"] = {"$gte": date_from, "$lte": date_to}
        elif date_from:
            query["date"] = {"$gte": date_from}
        elif date_to:
            query["date"] = {"$lte": date_to}

        skip = (page - 1) * page_size
        find_cmd = {
            "find": "attendance_logs",
            "filter": query,
            "sort": {"date": -1, "emp_code": 1},
            "skip": skip,
            "limit": page_size
        }
        collection_name = "attendance_logs"
        explain_res = db.command("explain", find_cmd, verbosity="executionStats")

    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="emp_code and month are required")
        year, m = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m)[1]
        pipeline = build_employee_monthly_pipeline(emp_code, month, num_days)
        agg_cmd = {
            "aggregate": "attendance_logs",
            "pipeline": pipeline,
            "cursor": {}
        }
        collection_name = "attendance_logs"
        explain_res = db.command("explain", agg_cmd, verbosity="executionStats")

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="month is required")
        year, m = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m)[1]
        pipeline = build_department_summary_pipeline(month, num_days, department)
        agg_cmd = {
            "aggregate": "employees",
            "pipeline": pipeline,
            "cursor": {}
        }
        collection_name = "employees"
        explain_res = db.command("explain", agg_cmd, verbosity="executionStats")

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="month is required")
        year, m = map(int, month.split("-"))
        num_days = calendar.monthrange(year, m)[1]
        pipeline = build_late_leaderboard_pipeline(month, num_days, department)
        agg_cmd = {
            "aggregate": "attendance_logs",
            "pipeline": pipeline,
            "cursor": {}
        }
        collection_name = "attendance_logs"
        explain_res = db.command("explain", agg_cmd, verbosity="executionStats")

    elif endpoint == "department_trend":
        if not department or not from_val or not to_val:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="department, from, and to are required")
        from_d = date.fromisoformat(from_val)
        to_d = date.fromisoformat(to_val)
        if to_d < from_d:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="to must be greater than or equal to from")
        if (to_d - from_d).days + 1 > 92:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="date range exceeds 92 days")

        pipeline = build_department_trend_pipeline(department, from_val, to_val)
        agg_cmd = {
            "aggregate": "attendance_logs",
            "pipeline": pipeline,
            "cursor": {}
        }
        collection_name = "attendance_logs"
        explain_res = db.command("explain", agg_cmd, verbosity="executionStats")

    clean_explain = json.loads(json_util.dumps(explain_res))
    return {
        "endpoint": endpoint,
        "collection": collection_name,
        "explain": clean_explain
    }