# NoSQL Schema Design (MongoDB)


## Mapping table (SQL -> MongoDB)

### Collections 
1. `patients` is the main collection based on the `patients` table. 
  The fields are subject_id, gender, dob, dod, expire_flag.
```json
{
    "_id": subject_id <- from table `patients`
    "gender": "F",
    "dob": ISODate("..."),
    "dod": ISODate("..."),
    "expire_flag": 1,
  }
```

2. `noteevents` is another collection which stores all the notes information such as note_id, subject_id, ham_id, chartdate, charttime, category, description, text, etc ( there's a lot more )
```json
{
  "_id": note_id <- from table `noteevents`
  "subject_id": 10120, 
  "ham_id":"",
  ... etc
}
```
### Embedded arrays (1-to-many relationships)

1. `admissions`  embedded array in patients `patients.admissions[]`
*Embed based on: admissions.subject_id = patients.subject_id*
Each admission is saved within patients as a sub-document with its columns becoming the attributes:
hadm_id, admittime, dischtime, admission_type, diagnosis, etc.. 
```json
  PATIENTS COLLECTION 
 {
    "_id": subject_id <- from table `patients`
    "gender": "F",
    "dob": ISODate("..."),
    "dod": ISODate("..."),
    "expire_flag": 1,
    
    --- EMBEDDED ARRAY ---
    "admissions": [
          {
            "hadm_id": 142345,
            "admittime": ISODate("..."),
            "dischtime": ISODate("..."),
            "admission_type": "EMERGENCY",
            "diagnosis": "SEPSIS",
            "hospital_expire_flag": 0,
            etc
          }, ...] <--- rest of admissions

  }
```

2. `icustays` embedded array inside of `admissions`.
*Embed based on: icustays.hadm_id = admissions.hadm_id (ICU stays belong to a specific admission)*
*Note: icustays.subject_id also matches the parent patient's subject_id (inherited through admission)*
For each admission, we embedd the number of icustays and its attributes: icustay_id, intime, outtime, first_careunit, last_careunit, los, etc
```json
  PATIENTS COLLECTION 
 {
    "_id": subject_id <- from table `patients`
    "gender": "F",
    "dob": ISODate(...),
    "dod": ISODate(...),
    "expire_flag": 1,
    "admissions": [
          {
            "hadm_id": 142345,
            "admittime": ISODate("..."),
            "dischtime": ISODate("..."),
            "admission_type": "EMERGENCY",
            "diagnosis": "SEPSIS",
            "hospital_expire_flag": 0,

              ---- EMBEDDED ARRAY ----
            "icustays": [
                {
                "icustay_id": 12312,
                "dbsource": "carevue",
                "first_careunit": "MICU",
                "intime": ISODate(...),
                "outtime": ISODate(...),
                "los": 1
                }, ....
              ] <--- rest of icustays

          }, ...] 

  }

```
3. `diagnoses_icd` embedded array inside of `admissions`.
*Embed based on: diagnoses_icd.hadm_id = admissions.hadm_id (diagnoses belong to a specific admission)*
For each icd9_code we also extract the short_title and long_title from table `d_icd_diagnoses`

### Denormalized data (lookup tables merged into documents)

- **`d_icd_diagnoses`** table is denormalized into the `diagnoses_icd` embedded array
  - Instead of storing only `icd9_code` and doing lookups, we copy `short_title` and `long_title` directly into each diagnosis
```json
  PATIENTS COLLECTION 
 {
    "_id": subject_id <- from table `patients`
    "gender": "F",
    "dob": ISODate(...),
    "dod": ISODate(...),
    "expire_flag": 1,
    "admissions": [
          {
            "hadm_id": 142345,
            "admittime": ISODate("..."),
            "dischtime": ISODate("..."),
            "admission_type": "EMERGENCY",
            "diagnosis": "SEPSIS",
            "hospital_expire_flag": 0,
            "icustays": [{
                "icustay_id": 12312,
                "dbsource": "carevue",
                "first_careunit": "MICU",
                "intime": ISODate(...),
                "outtime": ISODate(...),
                "los": 1
                }, ....],

            ---- EMBEDDED ARRAY ----
            "diagnoses_icd":[
                { "seq_num": 1 <-- comes from original diagnoses_icd
                , "icd9_code": "003.1", <-- comes from diagnoses_icd
                "long_title": " Cholera due to vibrio cholerae", <- comes from d_icd_diagnoses
                 "short_title": "Cholera" <-- also from  d_icd_diagnoses
                 },
                 .... <-- the other diagnoses_icd   
                ]



          }, ...] 

  }

```



## Why embed vs collection
- We put icustays and diagnoses as embedded arrays since they are always read together with the parent which is `patients`. 
- We put patients and noteevents as collections since they are large records that are pretty independent. 


