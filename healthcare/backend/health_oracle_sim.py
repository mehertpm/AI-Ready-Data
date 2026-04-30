"""
Simulates an Oracle EHR/clinical database (Epic-style) using SQLite.
8 clinical tables with realistic healthcare data and intentional DQ issues:
PHI exposure, invalid clinical codes, date coherence errors, lab outliers,
medication dosage violations, and duplicate patient records.
"""
import sqlite3, random, os, datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "health_oracle_sim.db")
random.seed(2025)

# ── Reference Data ────────────────────────────────────────────────────────────
FIRST_NAMES_M = ["James","Robert","John","Michael","David","William","Richard","Joseph",
                  "Thomas","Charles","Daniel","Anthony","Mark","Christopher","Matthew"]
FIRST_NAMES_F = ["Mary","Patricia","Jennifer","Linda","Barbara","Susan","Jessica","Sarah",
                  "Karen","Lisa","Nancy","Betty","Margaret","Sandra","Ashley"]
LAST_NAMES = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
              "Rodriguez","Martinez","Wilson","Anderson","Taylor","Thomas","Moore"]
STATES = ["CA","NY","TX","FL","IL","PA","OH","GA","NC","MI","WA","AZ","MA","TN","IN"]
CITIES = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
          "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville"]
BLOOD_TYPES = ["A+","A-","B+","B-","AB+","AB-","O+","O-"]
LANGUAGES = ["English","Spanish","Mandarin","French","Arabic","Vietnamese","Korean","Russian"]
ETHNICITIES = ["Non-Hispanic","Hispanic","Unknown","Declined"]
RACES = ["White","Black/African American","Asian","American Indian","Pacific Islander","Unknown","Declined"]

# Valid ICD-10-CM codes (common diagnoses)
VALID_ICD10 = [
    ("E11.9","Type 2 diabetes mellitus without complications"),
    ("I10","Essential (primary) hypertension"),
    ("J18.9","Pneumonia, unspecified organism"),
    ("K21.0","Gastro-esophageal reflux disease with esophagitis"),
    ("M54.5","Low back pain"),
    ("F32.9","Major depressive disorder, single episode, unspecified"),
    ("N18.3","Chronic kidney disease, stage 3"),
    ("J44.1","Chronic obstructive pulmonary disease with acute exacerbation"),
    ("I25.10","Atherosclerotic heart disease of native coronary artery without angina"),
    ("E78.5","Hyperlipidemia, unspecified"),
    ("J06.9","Acute upper respiratory infection, unspecified"),
    ("G43.909","Migraine, unspecified, not intractable, without status migrainosus"),
    ("A09","Infectious gastroenteritis and colitis, unspecified"),
    ("Z87.891","Personal history of nicotine dependence"),
    ("F41.9","Anxiety disorder, unspecified"),
    ("I48.91","Unspecified atrial fibrillation"),
    ("E03.9","Hypothyroidism, unspecified"),
    ("K57.30","Diverticulosis of large intestine without perforation or abscess"),
    ("M17.11","Primary osteoarthritis, right knee"),
    ("J45.20","Mild intermittent asthma, uncomplicated"),
    ("N39.0","Urinary tract infection, site not specified"),
    ("Z79.4","Long-term (current) use of insulin"),
    ("I50.9","Heart failure, unspecified"),
    ("C34.10","Malignant neoplasm of upper lobe, bronchus or lung, unspecified"),
    ("S72.001A","Fracture of unspecified part of neck of right femur, initial encounter"),
]
INVALID_ICD10 = ["E11999","XYZ123","J18.","I10X99","INVALID","","E999.99","K21",""]

# Valid CPT codes
VALID_CPT = ["99213","99214","99215","99232","99233","71046","71045","93000","93010",
             "80053","80061","85025","85027","36415","99285","99291","99292",
             "43239","47562","27447","90715","90686","G0463","99203","99204"]
INVALID_CPT = ["XXXXX","12","9999999","AB123","","00000"]

# Valid LOINC codes (lab tests)
VALID_LOINC = [
    ("2093-3","Cholesterol [Mass/volume] in Serum or Plasma","mg/dL",100,200,240),
    ("2571-8","Triglycerides [Mass/volume] in Serum or Plasma","mg/dL",50,150,500),
    ("718-7","Hemoglobin [Mass/volume] in Blood","g/dL",11.0,17.5,20.0),
    ("4548-4","Hemoglobin A1c/Hemoglobin.total in Blood","%",4.0,6.5,15.0),
    ("2160-0","Creatinine [Mass/volume] in Serum or Plasma","mg/dL",0.5,1.2,15.0),
    ("1742-6","Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma","U/L",7,56,3000),
    ("1920-8","Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma","U/L",10,40,5000),
    ("777-3","Platelets [#/volume] in Blood by Automated count","10*3/uL",150,400,1500),
    ("26464-8","Leukocytes [#/volume] in Blood","10*3/uL",4.0,11.0,100),
    ("14682-9","Sodium [Moles/volume] in Serum or Plasma","mEq/L",135,145,180),
    ("2823-3","Potassium [Moles/volume] in Serum or Plasma","mEq/L",3.5,5.0,8.0),
    ("33037-3","Glucose [Mass/volume] in Blood","mg/dL",60,100,600),
    ("3094-0","Urea nitrogen [Mass/volume] in Serum or Plasma","mg/dL",7,20,150),
    ("6768-6","Alkaline phosphatase [Enzymatic activity/volume] in Serum or Plasma","U/L",44,147,1000),
    ("49765-1","Calcium [Mass/volume] in Blood","mg/dL",8.5,10.5,14.0),
]
INVALID_LOINC = ["9999-9","INVALID","","LOINC-123","0000-0"]

# Valid NDC formats (drug codes)
NDC_DRUGS = [
    ("00074-3799-13","Metformin 500mg","500","mg","BID","PO"),
    ("00069-0150-30","Lisinopril 10mg","10","mg","QD","PO"),
    ("59762-0001-01","Atorvastatin 20mg","20","mg","QHS","PO"),
    ("00003-0892-20","Amlodipine 5mg","5","mg","QD","PO"),
    ("00085-1328-01","Omeprazole 20mg","20","mg","QD","PO"),
    ("57894-0015-30","Levothyroxine 50mcg","50","mcg","QD","PO"),
    ("00071-0155-24","Sertraline 50mg","50","mg","QD","PO"),
    ("00378-5074-91","Gabapentin 300mg","300","mg","TID","PO"),
    ("00093-7149-56","Albuterol 90mcg","2","puffs","Q4H PRN","INH"),
    ("00069-0031-68","Insulin glargine 100unit/mL","20","units","QHS","SQ"),
    ("00006-0963-54","Warfarin 5mg","5","mg","QD","PO"),
    ("50458-0579-30","Apixaban 5mg","5","mg","BID","PO"),
    ("00093-0058-01","Hydrochlorothiazide 25mg","25","mg","QD","PO"),
    ("00069-0172-66","Azithromycin 250mg","250","mg","QD","PO"),
    ("16590-0618-30","Amoxicillin 500mg","500","mg","TID","PO"),
]
INVALID_NDC = ["INVALID-NDC","12345","DRUG123",""]

SPECIALTIES = ["Internal Medicine","Family Medicine","Cardiology","Pulmonology",
               "Nephrology","Endocrinology","Gastroenterology","Oncology",
               "Orthopedics","Neurology","Psychiatry","Emergency Medicine",
               "Radiology","Anesthesiology","Surgery"]
TAXONOMY_CODES = ["207R00000X","207Q00000X","207RC0000X","207RP1001X",
                  "207RN0300X","207RE0101X","207RG0100X","2086S0122X"]
ENCOUNTER_TYPES = ["OUTPATIENT","INPATIENT","EMERGENCY","OBSERVATION","TELEHEALTH","SURGERY"]
DISCHARGE_DISPS = ["Home","Skilled Nursing Facility","Rehab Facility","Home Health","Expired","AMA","Transferred"]
ADMISSION_SOURCES = ["Physician Referral","Emergency Room","Transfer","Self-Referral","Court/Law Enforcement"]
DRG_CODES = ["470","291","292","293","194","195","871","872","689","690","247","248","149","150","637"]
CLAIM_STATUSES = ["PAID","DENIED","PENDING","PARTIAL","VOIDED"]
DENIAL_REASONS = ["Prior Auth Required","Not Covered","Duplicate Claim","Missing Info","Out of Network","Exceeded Limit"]
PAYERS = ["Medicare","Medicaid","BlueCross BlueShield","Aetna","UnitedHealth","Cigna","Humana","Kaiser","Self-Pay"]

# 18 HIPAA PHI identifiers tracked
PHI_IDENTIFIERS_18 = [
    "Names","Geographic data (sub-state)","Dates (except year)","Phone numbers",
    "Fax numbers","Email addresses","Social Security Numbers","Medical Record Numbers",
    "Health plan beneficiary numbers","Account numbers","Certificate/license numbers",
    "Vehicle identifiers","Device identifiers","URLs","IP addresses",
    "Biometric identifiers","Full-face photographs","Any unique identifying number"
]


def _rand_date(start_year=2018, end_year=2024):
    s = datetime.date(start_year, 1, 1)
    e = datetime.date(end_year, 12, 31)
    return (s + datetime.timedelta(days=random.randint(0, (e - s).days))).isoformat()


def _rand_dob(min_age=1, max_age=95):
    today = datetime.date(2024, 12, 31)
    age = random.randint(min_age, max_age)
    return (today - datetime.timedelta(days=age * 365 + random.randint(0, 364))).isoformat()


def _rand_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def _rand_npi_valid():
    # Generate a plausible 10-digit NPI (not Luhn-validated but correct format)
    return f"1{random.randint(100000000,999999999)}"


def _rand_phone():
    return f"({random.randint(200,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}"


def _maybe_null(val, pct=0.10):
    return None if random.random() < pct else val


def _invalid_icd10(pct=0.15):
    return random.choice(INVALID_ICD10) if random.random() < pct else None


def _invalid_cpt(pct=0.10):
    return random.choice(INVALID_CPT) if random.random() < pct else None


def create_database():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ── PATIENTS (5,000 rows) ─────────────────────────────────────────────────
    c.execute("""CREATE TABLE PATIENTS (
        patient_id TEXT PRIMARY KEY, mrn TEXT,
        first_name TEXT, last_name TEXT,
        gender TEXT, dob TEXT, ssn TEXT,
        address TEXT, city TEXT, state TEXT, zip TEXT,
        phone TEXT, email TEXT,
        blood_type TEXT, language TEXT, race TEXT, ethnicity TEXT,
        insurance_id TEXT, payer TEXT,
        emergency_contact TEXT,
        created_date TEXT, updated_date TEXT
    )""")
    patients, mrn_pool = [], set()
    pid_pool = [f"PAT-{i+1:07d}" for i in range(5000)]
    for i in range(5000):
        gender = random.choice(["M","F"])
        first = random.choice(FIRST_NAMES_M if gender=="M" else FIRST_NAMES_F)
        last  = random.choice(LAST_NAMES)
        mrn   = f"MRN-{random.randint(1000000,9999999)}"
        # ~5% duplicate MRNs
        if random.random() < 0.05 and mrn_pool:
            mrn = random.choice(list(mrn_pool))
        mrn_pool.add(mrn)
        dob = _rand_dob()
        # ~3% impossible DOB (future date)
        if random.random() < 0.03:
            dob = _rand_date(2025, 2026)
        patients.append((
            pid_pool[i], mrn, first, last, gender, dob,
            _maybe_null(_rand_ssn(), 0.08),
            f"{random.randint(100,9999)} {random.choice(['Oak','Elm','Main','Park','Cedar','Maple'])} {random.choice(['St','Ave','Blvd','Dr','Ln'])}",
            random.choice(CITIES), random.choice(STATES),
            f"{random.randint(10000,99999)}",
            _maybe_null(_rand_phone(), 0.10),
            _maybe_null(f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}", 0.12),
            random.choice(BLOOD_TYPES),
            random.choice(LANGUAGES),
            random.choice(RACES), random.choice(ETHNICITIES),
            f"INS-{random.randint(100000000,999999999)}",
            random.choice(PAYERS),
            _maybe_null(f"{random.choice(FIRST_NAMES_M+FIRST_NAMES_F)} {random.choice(LAST_NAMES)}", 0.15),
            _rand_date(2010, 2023), _maybe_null(_rand_date(2023, 2024), 0.20),
        ))
    c.executemany("INSERT OR IGNORE INTO PATIENTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", patients)

    # ── PROVIDERS (1,000 rows) ────────────────────────────────────────────────
    c.execute("""CREATE TABLE PROVIDERS (
        provider_id TEXT PRIMARY KEY, npi TEXT,
        first_name TEXT, last_name TEXT,
        specialty TEXT, taxonomy_code TEXT,
        facility_id TEXT, state_license TEXT, dea_number TEXT,
        phone TEXT, email TEXT, credential TEXT, status TEXT
    )""")
    providers = []
    prov_pool = [f"PROV-{i+1:05d}" for i in range(1000)]
    for i in range(1000):
        first = random.choice(FIRST_NAMES_M + FIRST_NAMES_F)
        last  = random.choice(LAST_NAMES)
        npi   = _rand_npi_valid()
        # ~8% invalid NPI format
        if random.random() < 0.08:
            npi = f"{random.randint(100,999)}"   # too short
        cred  = random.choice(["MD","DO","NP","PA","RN","PhD"])
        providers.append((
            prov_pool[i], npi, first, last,
            random.choice(SPECIALTIES), random.choice(TAXONOMY_CODES),
            f"FAC-{random.randint(1000,9999)}",
            _maybe_null(f"{random.choice(STATES)}-{random.randint(10000,99999)}", 0.05),
            _maybe_null(f"B{random.randint(1000000,9999999)}", 0.10),
            _rand_phone(),
            f"{first.lower()}.{last.lower()}@hospital.org",
            cred, random.choice(["ACTIVE","INACTIVE","SUSPENDED"]),
        ))
    c.executemany("INSERT OR IGNORE INTO PROVIDERS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", providers)

    # ── ENCOUNTERS (20,000 rows) ──────────────────────────────────────────────
    c.execute("""CREATE TABLE ENCOUNTERS (
        encounter_id TEXT PRIMARY KEY,
        patient_id TEXT, provider_id TEXT, facility_id TEXT,
        admit_date TEXT, discharge_date TEXT,
        encounter_type TEXT, chief_complaint TEXT,
        disposition TEXT, admission_source TEXT,
        drg_code TEXT, los_days REAL, status TEXT,
        created_date TEXT
    )""")
    complaints = ["Chest pain","Shortness of breath","Abdominal pain","Fever and chills",
                  "Back pain","Headache","Dizziness","Nausea/vomiting","Falls","Confusion",
                  "Wound care","Routine follow-up","Annual physical","Medication review"]
    encounters = []
    enc_pool = [f"ENC-{i+1:08d}" for i in range(20000)]
    for i in range(20000):
        pid   = random.choice(pid_pool)
        provid = random.choice(prov_pool)
        # ~5% invalid patient FK
        if random.random() < 0.05:
            pid = f"PAT-INVALID-{i}"
        admit = _rand_date(2020, 2024)
        adm_dt = datetime.date.fromisoformat(admit)
        los   = random.uniform(0, 30)
        disch_dt = adm_dt + datetime.timedelta(days=los)
        discharge = disch_dt.isoformat()
        # ~8% null discharge
        discharge = _maybe_null(discharge, 0.08)
        # ~5% admit > discharge (date coherence error)
        if random.random() < 0.05 and discharge:
            discharge = (adm_dt - datetime.timedelta(days=random.randint(1,5))).isoformat()
        encounters.append((
            enc_pool[i], pid, provid, f"FAC-{random.randint(1000,9999)}",
            admit, discharge,
            random.choice(ENCOUNTER_TYPES), random.choice(complaints),
            _maybe_null(random.choice(DISCHARGE_DISPS), 0.08),
            random.choice(ADMISSION_SOURCES),
            _maybe_null(random.choice(DRG_CODES), 0.10),
            round(los, 1) if discharge else None,
            random.choice(["COMPLETED","IN_PROGRESS","CANCELLED"]),
            admit,
        ))
    c.executemany("INSERT OR IGNORE INTO ENCOUNTERS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", encounters)

    # ── DIAGNOSES (35,000 rows) ───────────────────────────────────────────────
    c.execute("""CREATE TABLE DIAGNOSES (
        diagnosis_id TEXT PRIMARY KEY,
        encounter_id TEXT, patient_id TEXT,
        icd10_code TEXT, icd10_description TEXT,
        diagnosis_type TEXT, onset_date TEXT,
        resolution_date TEXT, severity TEXT, provider_id TEXT
    )""")
    diag_types = ["PRIMARY","SECONDARY","COMORBIDITY","ADMITTING","DISCHARGE"]
    severities = ["MILD","MODERATE","SEVERE","CRITICAL"]
    diagnoses = []
    for i in range(35000):
        enc_id = random.choice(enc_pool)
        pid    = random.choice(pid_pool)
        icd    = random.choice(VALID_ICD10)
        code, desc = icd
        # ~15% invalid ICD-10 codes
        bad = _invalid_icd10(0.15)
        if bad is not None:
            code = bad; desc = "UNMAPPED"
        onset = _rand_date(2018, 2024)
        onset_dt = datetime.date.fromisoformat(onset)
        resol = (onset_dt + datetime.timedelta(days=random.randint(1, 365))).isoformat()
        resol = _maybe_null(resol, 0.20)
        # ~5% onset > resolution (date coherence error)
        if resol and random.random() < 0.05:
            resol = (onset_dt - datetime.timedelta(days=random.randint(1,30))).isoformat()
        diagnoses.append((
            f"DX-{i+1:08d}", enc_id, pid,
            code, desc,
            random.choice(diag_types), onset,
            resol, random.choice(severities),
            random.choice(prov_pool),
        ))
    c.executemany("INSERT OR IGNORE INTO DIAGNOSES VALUES (?,?,?,?,?,?,?,?,?,?)", diagnoses)

    # ── MEDICATIONS (15,000 rows) ─────────────────────────────────────────────
    c.execute("""CREATE TABLE MEDICATIONS (
        medication_id TEXT PRIMARY KEY,
        patient_id TEXT, encounter_id TEXT, provider_id TEXT,
        drug_name TEXT, ndc_code TEXT,
        dose TEXT, dose_unit TEXT,
        frequency TEXT, route TEXT,
        start_date TEXT, end_date TEXT,
        rxnorm_code TEXT, status TEXT, refills INTEGER
    )""")
    med_statuses = ["ACTIVE","DISCONTINUED","COMPLETED","ON_HOLD","CANCELLED"]
    medications = []
    used_med_ids = set()
    for i in range(15000):
        pid    = random.choice(pid_pool)
        enc_id = random.choice(enc_pool)
        drug   = random.choice(NDC_DRUGS)
        ndc, name, dose, unit, freq, route = drug
        # ~12% null NDC
        ndc = _maybe_null(ndc, 0.12)
        # ~5% invalid NDC
        if ndc and random.random() < 0.05:
            ndc = random.choice(INVALID_NDC)
        # ~8% dosage multiplied way out of range (safety issue)
        if random.random() < 0.08:
            dose = str(int(float(dose) * random.randint(10, 50)))
        start = _rand_date(2020, 2024)
        end   = _maybe_null(_rand_date(2023, 2025), 0.15)
        mid   = f"MED-{i+1:07d}"
        # ~3% duplicate medication orders
        if random.random() < 0.03 and used_med_ids:
            mid = random.choice(list(used_med_ids))
        else:
            used_med_ids.add(mid)
        medications.append((
            mid, pid, enc_id, random.choice(prov_pool),
            name, ndc, dose, unit, freq, route,
            start, end,
            f"RX{random.randint(100000,999999)}",
            random.choice(med_statuses), random.randint(0, 5),
        ))
    c.executemany("INSERT OR IGNORE INTO MEDICATIONS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", medications)

    # ── LAB_RESULTS (40,000 rows) ─────────────────────────────────────────────
    c.execute("""CREATE TABLE LAB_RESULTS (
        lab_id TEXT PRIMARY KEY,
        patient_id TEXT, encounter_id TEXT,
        loinc_code TEXT, test_name TEXT,
        result_value REAL, result_unit TEXT,
        reference_low REAL, reference_high REAL,
        result_flag TEXT, collection_date TEXT,
        result_date TEXT, performing_lab TEXT, status TEXT
    )""")
    labs, flags_map = [], {"H": "High", "L": "Low", "N": "Normal", "C": "Critical", "PD": "Panic Delta"}
    for i in range(40000):
        pid    = random.choice(pid_pool)
        enc_id = random.choice(enc_pool)
        loinc  = random.choice(VALID_LOINC)
        lcode, lname, lunit, ref_lo, ref_hi, crit_hi = loinc
        # ~8% invalid LOINC
        if random.random() < 0.08:
            lcode = random.choice(INVALID_LOINC); lname = "UNMAPPED"
        # Generate value — mostly normal, some H/L, some critical
        rv = random.gauss((ref_lo + ref_hi) / 2, (ref_hi - ref_lo) / 4)
        # ~10% clinically impossible values (>3× reference high or negative)
        if random.random() < 0.10:
            rv = random.choice([rv * random.uniform(3, 10), -abs(rv)])
        rv = round(rv, 2)
        flag = "N"
        if rv > ref_hi: flag = "C" if rv > crit_hi else "H"
        elif rv < ref_lo: flag = "L" if rv > 0 else "C"
        coll = _rand_date(2020, 2024)
        coll_dt = datetime.date.fromisoformat(coll)
        result_dt = (coll_dt + datetime.timedelta(hours=random.randint(1, 72))).isoformat()
        # ~5% result before collection (date error)
        if random.random() < 0.05:
            result_dt = (coll_dt - datetime.timedelta(days=random.randint(1,5))).isoformat()
        labs.append((
            f"LAB-{i+1:08d}", pid, enc_id,
            lcode, lname, rv, lunit, ref_lo, ref_hi, flag,
            coll, result_dt,
            _maybe_null(f"LabCorp-{random.randint(100,999)}", 0.05),
            random.choice(["FINAL","PRELIMINARY","CORRECTED","PENDING"]),
        ))
    c.executemany("INSERT OR IGNORE INTO LAB_RESULTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", labs)

    # ── PROCEDURES (12,000 rows) ──────────────────────────────────────────────
    c.execute("""CREATE TABLE PROCEDURES (
        procedure_id TEXT PRIMARY KEY,
        encounter_id TEXT, patient_id TEXT, provider_id TEXT,
        cpt_code TEXT, procedure_name TEXT,
        procedure_date TEXT, anesthesia_type TEXT,
        duration_minutes INTEGER, status TEXT, laterality TEXT
    )""")
    proc_names = ["Laparoscopic cholecystectomy","Total knee replacement","Colonoscopy",
                  "Upper endoscopy","Coronary angiography","Appendectomy","Hip replacement",
                  "Cataract removal","Cardiac catheterization","CT abdomen/pelvis",
                  "MRI brain without contrast","Chest X-ray PA/LAT","ECG 12-lead",
                  "Blood draw venipuncture","Flu vaccine administration"]
    anesthesia = ["General","Local","Regional","Spinal","Monitored Anesthesia Care","None"]
    procedures = []
    for i in range(12000):
        enc_id = random.choice(enc_pool)
        pid    = random.choice(pid_pool)
        cpt    = random.choice(VALID_CPT)
        # ~10% invalid CPT
        bad_cpt = _invalid_cpt(0.10)
        if bad_cpt is not None:
            cpt = bad_cpt
        proc_date = _rand_date(2020, 2024)
        # ~3% future procedure dates
        if random.random() < 0.03:
            proc_date = _rand_date(2025, 2026)
        procedures.append((
            f"PROC-{i+1:07d}", enc_id, pid, random.choice(prov_pool),
            cpt, random.choice(proc_names), proc_date,
            random.choice(anesthesia), random.randint(10, 480),
            random.choice(["COMPLETED","CANCELLED","IN_PROGRESS"]),
            _maybe_null(random.choice(["LEFT","RIGHT","BILATERAL","N/A"]), 0.20),
        ))
    c.executemany("INSERT OR IGNORE INTO PROCEDURES VALUES (?,?,?,?,?,?,?,?,?,?,?)", procedures)

    # ── CLAIMS (10,000 rows) ──────────────────────────────────────────────────
    c.execute("""CREATE TABLE CLAIMS (
        claim_id TEXT PRIMARY KEY,
        patient_id TEXT, encounter_id TEXT,
        provider_id TEXT, payer TEXT,
        claim_date TEXT, service_date TEXT, claim_type TEXT,
        billed_amount REAL, allowed_amount REAL,
        paid_amount REAL, patient_liability REAL,
        claim_status TEXT, denial_reason TEXT, npi TEXT
    )""")
    claim_types = ["PROFESSIONAL","INSTITUTIONAL","DENTAL","PHARMACY"]
    claims = []
    used_claim_ids = set()
    for i in range(10000):
        pid    = random.choice(pid_pool)
        enc_id = random.choice(enc_pool)
        billed = round(random.uniform(50, 50000), 2)
        allowed = round(billed * random.uniform(0.4, 0.9), 2)
        paid   = round(allowed * random.uniform(0.7, 1.0), 2)
        # ~8% paid > billed (financial integrity error)
        if random.random() < 0.08:
            paid = round(billed * random.uniform(1.1, 2.0), 2)
        pat_liab = round(max(0, allowed - paid), 2)
        status = random.choice(CLAIM_STATUSES)
        denial = None
        if status == "DENIED":
            denial = random.choice(DENIAL_REASONS)
        # ~10% denied with no denial reason
        elif status == "DENIED" and random.random() < 0.10:
            denial = None
        npi = _rand_npi_valid()
        if random.random() < 0.05:
            npi = f"{random.randint(100,999)}"  # invalid NPI
        cid = f"CLM-{i+1:07d}"
        # ~2% duplicate claims
        if random.random() < 0.02 and used_claim_ids:
            cid = random.choice(list(used_claim_ids))
        else:
            used_claim_ids.add(cid)
        service_dt = _rand_date(2020, 2024)
        # ~5% claim date before service date
        claim_dt_obj = datetime.date.fromisoformat(service_dt)
        claim_dt = (claim_dt_obj + datetime.timedelta(days=random.randint(1,30))).isoformat()
        if random.random() < 0.05:
            claim_dt = (claim_dt_obj - datetime.timedelta(days=random.randint(1,10))).isoformat()
        claims.append((
            cid, pid, enc_id, random.choice(prov_pool),
            random.choice(PAYERS), claim_dt, service_dt,
            random.choice(claim_types),
            billed, allowed, paid, pat_liab,
            status, denial, npi,
        ))
    c.executemany("INSERT OR IGNORE INTO CLAIMS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", claims)

    conn.commit()
    conn.close()
    return DB_PATH


def get_table_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    meta = {
        "PATIENTS":   {"display":"Patients",   "cols":22, "phi":["ssn","dob","email","phone","address","mrn","insurance_id"], "domain":"Demographics",      "fhir":"Patient"},
        "ENCOUNTERS": {"display":"Encounters", "cols":14, "phi":["patient_id"],                                               "domain":"Clinical Visits",    "fhir":"Encounter"},
        "DIAGNOSES":  {"display":"Diagnoses",  "cols":10, "phi":["patient_id"],                                               "domain":"ICD-10 Conditions",  "fhir":"Condition"},
        "MEDICATIONS":{"display":"Medications","cols":15, "phi":["patient_id"],                                               "domain":"Prescriptions",      "fhir":"MedicationRequest"},
        "LAB_RESULTS":{"display":"Lab Results","cols":14, "phi":["patient_id"],                                               "domain":"Observations",       "fhir":"Observation"},
        "PROCEDURES": {"display":"Procedures", "cols":11, "phi":["patient_id"],                                               "domain":"CPT Procedures",     "fhir":"Procedure"},
        "CLAIMS":     {"display":"Claims",     "cols":15, "phi":["patient_id","npi"],                                         "domain":"Insurance Claims",    "fhir":"Claim"},
        "PROVIDERS":  {"display":"Providers",  "cols":13, "phi":["npi","dea_number","email"],                                 "domain":"Practitioner",       "fhir":"Practitioner"},
    }
    result = {}
    for tbl, m in meta.items():
        row = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
        result[tbl] = {**m, "rows": row[0]}
    conn.close()
    return result


TABLE_ORDER = ["PATIENTS","ENCOUNTERS","DIAGNOSES","MEDICATIONS",
               "LAB_RESULTS","PROCEDURES","CLAIMS","PROVIDERS"]

# DQ issue rates per table (reflects injected problems)
TABLE_DQ_ISSUES = {
    "PATIENTS":   {"phi_missing_pct":10, "invalid_code_pct":0,  "date_error_pct":3,  "duplicate_pct":5,  "range_error_pct":0,  "claim_error_pct":0,  "fk_violation_pct":0},
    "ENCOUNTERS": {"phi_missing_pct":5,  "invalid_code_pct":0,  "date_error_pct":5,  "duplicate_pct":0,  "range_error_pct":0,  "claim_error_pct":0,  "fk_violation_pct":5},
    "DIAGNOSES":  {"phi_missing_pct":0,  "invalid_code_pct":15, "date_error_pct":5,  "duplicate_pct":0,  "range_error_pct":0,  "claim_error_pct":0,  "fk_violation_pct":0},
    "MEDICATIONS":{"phi_missing_pct":5,  "invalid_code_pct":12, "date_error_pct":0,  "duplicate_pct":3,  "range_error_pct":8,  "claim_error_pct":0,  "fk_violation_pct":0},
    "LAB_RESULTS":{"phi_missing_pct":0,  "invalid_code_pct":8,  "date_error_pct":5,  "duplicate_pct":0,  "range_error_pct":10, "claim_error_pct":0,  "fk_violation_pct":0},
    "PROCEDURES": {"phi_missing_pct":0,  "invalid_code_pct":10, "date_error_pct":3,  "duplicate_pct":0,  "range_error_pct":0,  "claim_error_pct":0,  "fk_violation_pct":0},
    "CLAIMS":     {"phi_missing_pct":0,  "invalid_code_pct":5,  "date_error_pct":5,  "duplicate_pct":2,  "range_error_pct":0,  "claim_error_pct":8,  "fk_violation_pct":0},
    "PROVIDERS":  {"phi_missing_pct":5,  "invalid_code_pct":8,  "date_error_pct":0,  "duplicate_pct":0,  "range_error_pct":0,  "claim_error_pct":0,  "fk_violation_pct":0},
}

# AI Readiness per layer (healthcare scoring: PHI compliance weighted heavily)
READINESS_SCORES = {
    "PATIENTS":   {"source":42,"bronze":46,"silver":72,"gold":89,"platinum":95},
    "ENCOUNTERS": {"source":55,"bronze":58,"silver":76,"gold":88,"platinum":94},
    "DIAGNOSES":  {"source":48,"bronze":51,"silver":74,"gold":91,"platinum":96},
    "MEDICATIONS":{"source":50,"bronze":54,"silver":75,"gold":89,"platinum":95},
    "LAB_RESULTS":{"source":57,"bronze":61,"silver":79,"gold":92,"platinum":97},
    "PROCEDURES": {"source":53,"bronze":57,"silver":77,"gold":90,"platinum":95},
    "CLAIMS":     {"source":60,"bronze":63,"silver":80,"gold":91,"platinum":94},
    "PROVIDERS":  {"source":65,"bronze":69,"silver":82,"gold":93,"platinum":97},
}

# HIPAA compliance score per layer (separate from AI readiness)
HIPAA_SCORES = {
    "PATIENTS":   {"bronze":28,"silver":71,"gold":95,"platinum":99},
    "ENCOUNTERS": {"bronze":52,"silver":79,"gold":96,"platinum":99},
    "DIAGNOSES":  {"bronze":60,"silver":82,"gold":97,"platinum":99},
    "MEDICATIONS":{"bronze":55,"silver":78,"gold":95,"platinum":99},
    "LAB_RESULTS":{"bronze":58,"silver":80,"gold":96,"platinum":99},
    "PROCEDURES": {"bronze":60,"silver":81,"gold":96,"platinum":99},
    "CLAIMS":     {"bronze":50,"silver":77,"gold":95,"platinum":99},
    "PROVIDERS":  {"bronze":62,"silver":84,"gold":97,"platinum":99},
}

SCORE_FACTORS = {
    "PATIENTS":   {"phi_completeness":55,"clinical_code_validity":98,"temporal_coherence":82,"referential_integrity":95,"deid_readiness":40},
    "ENCOUNTERS": {"phi_completeness":80,"clinical_code_validity":98,"temporal_coherence":78,"referential_integrity":82,"deid_readiness":68},
    "DIAGNOSES":  {"phi_completeness":95,"clinical_code_validity":72,"temporal_coherence":82,"referential_integrity":88,"deid_readiness":78},
    "MEDICATIONS":{"phi_completeness":78,"clinical_code_validity":75,"temporal_coherence":92,"referential_integrity":90,"deid_readiness":72},
    "LAB_RESULTS":{"phi_completeness":90,"clinical_code_validity":80,"temporal_coherence":88,"referential_integrity":92,"deid_readiness":82},
    "PROCEDURES": {"phi_completeness":92,"clinical_code_validity":78,"temporal_coherence":88,"referential_integrity":90,"deid_readiness":80},
    "CLAIMS":     {"phi_completeness":88,"clinical_code_validity":85,"temporal_coherence":82,"referential_integrity":88,"deid_readiness":75},
    "PROVIDERS":  {"phi_completeness":82,"clinical_code_validity":82,"temporal_coherence":98,"referential_integrity":95,"deid_readiness":85},
}

PHI_DETECTED = {
    "PATIENTS": {
        "Names":True, "Geographic data (sub-state)":True, "Dates (except year)":True,
        "Phone numbers":True, "Email addresses":True, "Social Security Numbers":True,
        "Medical Record Numbers":True, "Health plan beneficiary numbers":True,
        "Account numbers":True, "Fax numbers":False, "Certificate/license numbers":False,
        "Vehicle identifiers":False, "Device identifiers":False, "URLs":False,
        "IP addresses":False, "Biometric identifiers":False,
        "Full-face photographs":False, "Any unique identifying number":True,
    },
    "ENCOUNTERS": {
        "Names":False, "Geographic data (sub-state)":False, "Dates (except year)":True,
        "Phone numbers":False, "Email addresses":False, "Social Security Numbers":False,
        "Medical Record Numbers":True, "Health plan beneficiary numbers":False,
        "Account numbers":False, "Fax numbers":False, "Certificate/license numbers":False,
        "Vehicle identifiers":False, "Device identifiers":False, "URLs":False,
        "IP addresses":False, "Biometric identifiers":False,
        "Full-face photographs":False, "Any unique identifying number":True,
    },
}
# Fill remaining tables with minimal PHI
for _t in ["DIAGNOSES","MEDICATIONS","LAB_RESULTS","PROCEDURES","CLAIMS","PROVIDERS"]:
    PHI_DETECTED[_t] = {k: random.random()<0.25 for k in PHI_IDENTIFIERS_18}
