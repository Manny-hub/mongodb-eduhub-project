"""
EduHub — MongoDB queries & operations
=====================================

This Python script (PyMongo) collects the most useful MongoDB queries, aggregations,
indexing commands, schema validation snippets, and operational helpers for the
EduHub LMS project. Each section contains:
 - short explanation (as comments),
 - safe-to-run functions, and
 - detailed inline comments showing step-by-step execution flow.

HOW TO USE
----------
1. Install dependencies: `pip install pymongo python-dateutil faker`
2. Update MONGO_URI and DB_NAME below to point to your database.
3. By default functions are in "dry run" mode. Set `EXECUTE=True` only when you
   are ready to run writes against your database.
4. Open and run the sections you need.

Note: This file is educational and written to be verbose and explicit.
Remove or adapt sections before running in production.
"""

# --------------------------
# Configuration & Setup
# --------------------------
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import DuplicateKeyError, WriteError
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random
import time
import heapq

# Toggle to True to perform writes (inserts/updates). Keep False for read-only tests.
EXECUTE = False

# Connection config (change to your environment)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "EduHub"

# Connect
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Collections (single source of truth)
users_col = db["users"]
courses_col = db["courses"]
lessons_col = db["lessons"]
enroll_col = db["enrollments"]
assign_col = db["assignments"]
archive_col = db["enrollments_archive"]

# --------------------------
# Helper utilities
# --------------------------

def safe_print(title, data=None):
    print("\n" + "="*6 + f" {title} " + "="*6)
    if data is not None:
        print(data)


# --------------------------
# 1. Sample data insertion (users, courses, enrollments, assignments)
#    - Demonstrates correct field names/types
#    - Comments explain each field and potential pitfalls
# --------------------------

def insert_sample_users(n_students=16, n_instructors=4):
    """Insert sample users (students + instructors).
    Default: 16 students and 4 instructors.
    """
    from faker import Faker
    #user data 
    fake = Faker()
    
    # Adding students and instructors programmatically
    
    firstname = ["Adams", "John", "Cooper", "Doe", "Jane", "Smith", "Emily", "Johnson", "Oladele", "Michael", "Brown", "Sarah", "Williams", "David", "Adebayo", "Bamidele","Jones", "Linda", "Miller", "James"]
    
    lastname = ["Ruth", "Alice", "Davis", "Robert", "Mary", "Grace", "Patricia", "Martinez", "Jennifer", "Pelumi", "Elizabeth", "Betrice", "Barbara", "John", "Susan", "Wilson","Jessica", "Anderson", "Sarah", "Thomas"]
    
    skills = ["mathematics", "programming", "python", "sql", "excel", "analysis"]
    
    users = []
    
    for i in range(20):  # Adding 20 sample students
        user = {
            "_id" : ObjectId(), 
            "userId": f"user_{i + 1}",
            "firstname": random.choice(firstname),
            "lastname": random.choice(lastname),
            "role": "student" if i < 15 else "instructor",
            "dateJoined": datetime.now() - timedelta(days=random.randint(0, 500)),
            "profile": {
                "bio": fake.text(max_nb_chars=200),
                "avatar": fake.image_url(),
                "skills": random.sample(skills, random.randint(1, 5))
            },
            "is_active": random.choice([True, True, False])
        }
        email = f"{user['firstname'].lower()}.{user['lastname'].lower()}@eduhub.com" if user["role"] == "student" \
            else f"{user['firstname'].lower()}.{user['lastname'].lower()}@instructor.eduhub.com"
        user["email"] = email
        users.append(user)


def insert_many_documents(collection, documents):
    try:
        result = collection.insert_many(documents)
        return result.inserted_ids
    except Exception as e:
        print(f"An error occurred: {e}")
        return []




def insert_sample_courses():
    """Insert a small catalog of courses with tags, category, instructorId, duration (hours), price.
    Course document shape follows the schema described earlier in the project.
    """
    sample_courses = [
        {"courseId": "PYT", "title": "Python", "description": "Learn Python", "instructorId": "user_17",
         "category": "Programming", "level": "beginner", "duration": 40, "price": 100, "tags": ["python", "programming"],
         "createdAt": datetime.now(), "updatedAt": datetime.now(), "isPublished": True},
        {"courseId": "SQL", "title": "SQL", "description": "Learn SQL", "instructorId": "user_18",
         "category": "Database", "level": "beginner", "duration": 30, "price": 80, "tags": ["sql", "database"],
         "createdAt": datetime.now(), "updatedAt": datetime.now(), "isPublished": True},
        {"courseId": "DSI", "title": "Data Science", "description": "Intro to Data Science", "instructorId": "user_17",
         "category": "Data", "level": "intermediate", "duration": 80, "price": 200, "tags": ["data", "python", "analysis"],
         "createdAt": datetime.now(), "updatedAt": datetime.now(), "isPublished": True}
    ]

    if EXECUTE:
        res = courses_col.insert_many(sample_courses)
        safe_print("Inserted courses", len(res.inserted_ids))
    else:
        safe_print("Dry run - would insert courses", [c["courseId"] for c in sample_courses])


def insert_sample_enrollments():
    """Create enrollments linking students to courses. This demonstrates the enrollments bridge/junction.
    Each document: { enrollmentId, courseId, studentId, startDate, endDate }
    """
    students = list(users_col.find({"role": "student"}, {"userId": 1}))
    courses = list(courses_col.find({}, {"courseId": 1}))
    enrollments = []
    i = 1
    for s in students:
        # assign each student 1-3 random courses
        chosen = random.sample(courses, k=min(len(courses), random.randint(1, 3)))
        for c in chosen:
            enrollment = {
                "enrollmentId": f"enr_{i}",
                "courseId": c["courseId"],
                "studentId": s["userId"],
                "startDate": datetime.now() - timedelta(days=random.randint(0, 300)),
                "endDate": datetime.now() + timedelta(days=random.randint(30, 120)),
                "resources": [
                        { "type": "video", "url": "https://example.com/default_video.mp4" },
                        { "type": "pdf", "url": "https://example.com/default_doc.pdf" }
                    ]
            }
            enrollments.append(enrollment)
            i += 1

    if EXECUTE and enrollments:
        res = enroll_col.insert_many(enrollments)
        safe_print("Inserted enrollments", len(res.inserted_ids))
    else:
        safe_print("Dry run - would insert enrollments", len(enrollments))


def insert_sample_lessons():
    """Insert lessons with resources for each course."""
    
    courses = list(courses_col.find({}, {"courseId": 1, "title": 1}))
    lesson_docs = []

    for idx, c in enumerate(courses, start=1):
        lesson_id = f"L{idx}"
        lesson_doc = {
            "lessonId": lesson_id,
            "courseId": c["courseId"],
            "title": f"Lesson {idx} - {c['title']}",
            "resources": [
                {"type": "video", "url": f"https://example.com/{c['courseId']}_intro.mp4"},
                {"type": "pdf", "url": f"https://example.com/{c['courseId']}_notes.pdf"}
            ],
            "createdAt": datetime.now()
        }
        lesson_docs.append(lesson_doc)
    
    if EXECUTE and lesson_docs:
        res = lessons_col.insert_many(lesson_docs)
        safe_print("Inserted lessons:", len(res.inserted_ids))
    else:
        safe_print("Dry run - would insert lessons:", [d["lessonId"] for d in lesson_docs])


def insert_sample_assignments_and_grades():
    """Insert assignments where "grades" is a list of {studentId, grade} objects.
    This is the recommended, query-friendly pattern.
    """
    students = list(users_col.find({"role": "student"}, {"userId": 1}))
    courses = list(courses_col.find({}, {"courseId": 1}))

    docs = []
    for idx, c in enumerate(courses, start=1):
        grades = [{"studentId": s["userId"], "grade": random.randint(50, 100)} for s in students]
        doc = {
            "assignmentId": f"A{idx}",
            "courseId": c["courseId"],
            "description": f"Assignment for {c['courseId']}",
            "dueDate": datetime.now() + timedelta(days=random.randint(3, 30)),
            "grades": grades,
            "createdAt": datetime.now()
        }
        docs.append(doc)

    if EXECUTE and docs:
        res = assign_col.insert_many(docs)
        safe_print("Inserted assignments", len(res.inserted_ids))
    else:
        safe_print("Dry run - would insert assignments", [d["assignmentId"] for d in docs])


def insert_sample_submissions():
    """Insert submissions for each lesson and each student."""
    
    students = list(users_col.find({"role": "student"}, {"userId": 1}))
    lessons = list(lessons_col.find({}, {"lessonId": 1, "courseId": 1}))
    submission_docs = []

    for lesson in lessons:
        for s in students:
            submission_id = f"S_{lesson['lessonId']}_{s['userId']}"
            submission_doc = {
                "submissionId": submission_id,
                "lessonId": lesson["lessonId"],
                "studentId": s["userId"],
                "submittedAt": datetime.now() - timedelta(days=random.randint(0, 5)),
                "content": f"Submission for {lesson['lessonId']} by {s['userId']}",
                "grade": random.randint(60, 100),
                "fileUrl": f"https://example.com/submissions/{submission_id}.zip"
            }
            submission_docs.append(submission_doc)
    
    if EXECUTE and submission_docs:
        res = submissions_col.insert_many(submission_docs)
        safe_print("Inserted submissions:", len(res.inserted_ids))
    else:
        safe_print("Dry run - would insert submissions:", [d["submissionId"] for d in submission_docs])


# --------------------------
# 2. Index creation & text index
# --------------------------

def create_indexes():
    """Create recommended indexes for performance:
       - unique users.email
       - title text + category
       - assignments.dueDate
       - enrollments compound (studentId, courseId)
    """
    # users.email unique index
    try:
        users_col.create_index([("email", ASCENDING)], unique=True, name="idx_user_email")
        safe_print("Index created", "idx_user_email")
    except Exception as e:
        safe_print("Index create failed for users.email", str(e))

    # text index for courses: title + description + tags
    try:
        courses_col.create_index([("title", TEXT), ("description", TEXT), ("tags", TEXT)], name="idx_courses_text")
        safe_print("Text index created", "idx_courses_text")
    except Exception as e:
        safe_print("Index create failed for courses text", str(e))

    # assignments.dueDate
    try:
        assign_col.create_index([("dueDate", ASCENDING)], name="idx_assign_due")
        safe_print("Index created", "idx_assign_due")
    except Exception as e:
        safe_print("Index create failed for assignments.dueDate", str(e))

    # enrollments compound index
    try:
        enroll_col.create_index([("studentId", ASCENDING), ("courseId", ASCENDING)], name="idx_enroll_std_course")
        safe_print("Index created", "idx_enroll_std_course")
    except Exception as e:
        safe_print("Index create failed for enrollments", str(e))


# --------------------------
# 3. Aggregation examples — lookups, group, unwind
# --------------------------

def courses_with_instructors():
    """Show how to join courses -> instructors (users). Returns flattened course + instructor doc.
    Equivalent to SQL JOIN using $lookup + $unwind.
    """
    pipeline = [
        {"$lookup": {"from": "users", "localField": "instructorId", "foreignField": "userId", "as": "instructor"}},
        {"$unwind": {"path": "$instructor", "preserveNullAndEmptyArrays": True}},
        {"$project": {"courseId": 1, "title": 1, "category": 1, "instructor.userId": 1, "instructor.firstname": 1, "instructor.lastname": 1}}
    ]
    return list(courses_col.aggregate(pipeline))


def enrollments_per_course():
    """Group enrollments by courseId and count them (per-course totals)."""
    pipeline = [
        {"$group": {"_id": "$courseId", "totalEnrollments": {"$sum": 1}}},
        {"$sort": {"totalEnrollments": -1}}
    ]
    return list(enroll_col.aggregate(pipeline))


def enrollments_by_category():
    """Aggregate enrollments grouped by course category.
    Shows $lookup + $unwind + $group approach explained earlier.
    """
    pipeline = [
        {"$lookup": {"from": "courses", "localField": "courseId", "foreignField": "courseId", "as": "course_info"}},
        {"$unwind": "$course_info"},
        {"$group": {"_id": "$course_info.category", "totalEnrollments": {"$sum": 1}, "courses": {"$addToSet": "$course_info.title"}}},
        {"$sort": {"totalEnrollments": -1}}
    ]
    return list(enroll_col.aggregate(pipeline))


# --------------------------
# 4. Student performance analysis (grades aggregated from assignments.grades array)
# --------------------------

def student_performance_analysis():
    """Compute average grade per student and rank students using $setWindowFields (MongoDB 5+).
    Steps:
    - $unwind grades array
    - $group by studentId and average grades
    - $sort and $setWindowFields for rank
    """
    pipeline = [
        {"$unwind": "$grades"},
        {"$group": {"_id": "$grades.studentId", "avg_grade": {"$avg": "$grades.grade"}}},
        {"$sort": {"avg_grade": -1}},
        {"$setWindowFields": {"sortBy": {"avg_grade": -1}, "output": {"rank": {"$rank": {}}}}}
    ]
    return list(assign_col.aggregate(pipeline))


# --------------------------
# 5. Python recommender (content + popularity) — readable implementation
# --------------------------

def recommend_for_student_python(student_id, limit=10,
                                  tag_weight=3.0, category_weight=2.0, pop_weight=0.5,
                                  candidate_sample_size=None):
    """Python implementation mirroring the aggregation-based recommender.
    - Fetch student's enrolled courses
    - Build interest vector (tags, categories)
    - Score all non-enrolled candidates by tag overlap, category match, popularity
    """
    enrolled_course_ids = set(enroll_col.distinct("courseId", {"studentId": student_id}) or [])

    if not enrolled_course_ids:
        # fallback to globally popular
        pipeline = [
            {"$group": {"_id": "$courseId", "pop": {"$sum": 1}}},
            {"$sort": {"pop": -1}},
            {"$limit": limit},
            {"$lookup": {"from": "courses", "localField": "_id", "foreignField": "courseId", "as": "course"}},
            {"$unwind": "$course"}
        ]
        results = list(enroll_col.aggregate(pipeline))
        return [{"courseId": r["course"]["courseId"], "title": r["course"].get("title"), "popularity": r["pop"]} for r in results]

    enrolled_courses = list(courses_col.find({"courseId": {"$in": list(enrolled_course_ids)}}, {"tags": 1, "category": 1}))
    enrolled_tags = set()
    enrolled_categories = set()
    for c in enrolled_courses:
        for t in (c.get("tags") or []):
            enrolled_tags.add(t.lower())
        if c.get("category"):
            enrolled_categories.add(c["category"])

    candidate_query = {"courseId": {"$nin": list(enrolled_course_ids)}}
    candidates = list(courses_col.find(candidate_query, {"courseId": 1, "title": 1, "tags": 1, "category": 1, "price": 1}))

    # popularity for candidates
    candidate_ids = [c["courseId"] for c in candidates]
    pop_docs = list(enroll_col.aggregate([{"$match": {"courseId": {"$in": candidate_ids}}}, {"$group": {"_id": "$courseId", "count": {"$sum": 1}}}]))
    pop_map = {d["_id"]: d["count"] for d in pop_docs}

    scored = []
    for c in candidates:
        c_tags = set([t.lower() for t in (c.get("tags") or [])])
        tag_overlap = len(enrolled_tags & c_tags)
        category_match = 1 if c.get("category") in enrolled_categories else 0
        popularity = pop_map.get(c["courseId"], 0)
        score = tag_weight * tag_overlap + category_weight * category_match + pop_weight * popularity
        scored.append({"courseId": c["courseId"], "title": c.get("title"), "score": score, "popularity": popularity})

    top = heapq.nlargest(limit, scored, key=lambda x: (x["score"], x["popularity"]))
    return top


# --------------------------
# 6. Text search example (MongoDB text index usage)
# --------------------------

def text_search_courses(query, limit=20):
    """Search courses by free text (title, description, tags) using text index."""
    cursor = courses_col.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}, "title": 1, "courseId": 1, "tags": 1}).sort([("score", {"$meta": "textScore"})]).limit(limit)
    return list(cursor)


# --------------------------
# 7. Archiving old enrollments (batch safe copy-delete)
# --------------------------

def archive_old_enrollments(retention_years=2, batch_size=500):
    """Move enrollments older than retention_years to archive collection in batches.
    - Adds _archivedAt metadata to archived docs
    - Uses ordered=False to speed up bulk insert (skips duplicated during retries)
    """
    cutoff = datetime.now() - relativedelta(years=retention_years)
    query = {"startDate": {"$lte": cutoff}}
    total = 0

    while True:
        docs = list(enroll_col.find(query).limit(batch_size))
        if not docs:
            break
        for d in docs:
            d["_archivedAt"] = datetime.now()
        if EXECUTE:
            try:
                archive_col.insert_many(docs, ordered=False)
                ids = [d["_id"] for d in docs]
                res = enroll_col.delete_many({"_id": {"$in": ids}})
                total += res.deleted_count
                safe_print("Archived batch", res.deleted_count)
            except Exception as e:
                safe_print("Archiving error", str(e))
                break
        else:
            safe_print("Dry run - would archive batch of size", len(docs))
            break
    safe_print("Total archived", total)


# --------------------------
# 8. Schema validation examples (collMod) — run via db.command
# --------------------------

def apply_schema_validation():
    """Apply JSON schema validation to collections using db.command("collMod", ...)
    Run carefully — collMod requires admin privileges and can fail if documents violate schema.
    """
    user_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["firstname", "lastname", "email", "role"],
            "properties": {
                "firstname": {"bsonType": "string"},
                "lastname": {"bsonType": "string"},
                "email": {"bsonType": "string", "pattern": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"},
                "role": {"enum": ["student", "instructor", "admin"]}
            }
        }
    }

    try:
        if EXECUTE:
            db.command({"collMod": "users", "validator": user_validator, "validationLevel": "moderate"})
            safe_print("Applied schema validation to users")
        else:
            safe_print("Dry run - would apply user schema validation")
    except Exception as e:
        safe_print("collMod failed", str(e))


# --------------------------
# 9. Error handling examples for inserts
# --------------------------

def safe_insert_user(doc):
    try:
        res = users_col.insert_one(doc)
        safe_print("Inserted user _id", res.inserted_id)
    except DuplicateKeyError as e:
        safe_print("DuplicateKeyError", e.details)
    except WriteError as e:
        safe_print("WriteError", e.details)
    except Exception as e:
        safe_print("Unexpected insert error", str(e))



"""End of file

This script is intentionally verbose and documented so you can reuse sections for
individual tasks (indexing, aggregation, archiving, schema validation).

