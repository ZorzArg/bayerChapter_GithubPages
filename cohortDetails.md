# Cohort Details for EHDEN HMB

## Study Population

### Cohort 1: Natural Menopausal Women

**Description**: This cohort captures women between the ages of 40 and 65 with a first time diagnosis of menopause. We limit this cohort of women based on the following attrition: a) no hysterectomy or bilateral oophorectomy at any time prior to diagnosis, b) no exposure to endocrine therapy and c) no exposure to a hormonal therapy. Persons exit the cohort at age 65, as this is the natural end of menopause. This cohort captures the basic menopausal population to understand their treatment patterns. It is more specific than a general age cohort of women between age 40-65 because it relies on a menopausal diagnosis. Decision to include powered by prior research via EpiVaSym.

**Version 2** 07/14/2023

- remove concepts of family history of early/late menopause
- ignore observation period for hysterectomy and oophorectomy to ensure never took place anytime prior to menopause

**Version 1** 

- adaptation of cohort definition from EpiVaSym study

### Cohort 2: Natural Menopausal Women contraindicated for HT

**Description**: This cohort captures women between the ages of 40 and 65 with a first time diagnosis of menopause but who have a diagnosis of a condition which would make them ineligible for HT. These conditions include breast cancer, stroke, VTE, gynecological cancer, benign brain meningioma or MI. We limit this cohort of women based on the following attrition: a) no hysterectomy or bilateral oophorectomy at any time prior to diagnosis, b) no exposure to endocrine therapy and c) no exposure to a hormonal therapy. Persons exit the cohort at age 65, as this is the natural end of menopause.

This cohort serves as a subpopulation of the natural women cohort to understand treatment patterns of women who should not be prescribed hormone therapy. 

**Version 2** 07/14/2023

- ignore observation period for hysterectomy and oophorectomy to ensure never took place anytime prior to menopause

**Version 1** 

- adaptation of cohort definition from EpiVaSym study


### Cohort 3: Natural menopause with family history of breast cancer

**Description**: This cohort captures women between the ages of 40 and 65 with a first time diagnosis of menopause but who have a family history of breast cancer. We limit this cohort of women based on the following attrition: a) no hysterectomy or bilateral oophorectomy at any time prior to diagnosis, b) no exposure to endocrine therapy and c) no exposure to a hormonal therapy. Persons exit the cohort at age 65, as this is the natural end of menopause. 

Similar to those counter-indicated for HT, the population with a family history of breast cancer should have a different prescribing pattern. This cohort serves as an alternative subpopulation of menopausal women to observe treatment patterns. 

**Version 2** 07/14/2023

- ignore observation period for hysterectomy and oophorectomy to ensure never took place anytime prior to menopause
- ignore observation period for family history of breast cancer to ensure diagnosis recorded sometime prior to menopause

**Version 1** 

- adaptation of cohort definition from EpiVaSym study


### Cohort 4: Women with/at high risk of BC receiving endocrine therapy

**Description**: This cohort captures women who are exposed to endocrine therapy as a proxy for 'unnantural' menopause. These women must have a record of BC prior to endocrine therapy. Persons exit the cohort at age 65, as this is the natural end of menopause. 

The purpose of this cohort is to identify treatment patterns of menopausal women who have reached menopause due to a medical intervention instead of naturally. 

**Version 2** 07/14/2023

- correct mistake that endocrine therapy should be first time in patient history

**Version 1** 

- adaptation of cohort definition from EpiVaSym study


### Cohort 5: Women with VMS or sleep disturbance symptom

**Description**: This cohort definition is meant to be used for exploratory analysis where the index event is the diagnosis of a menopausal symptom such as VMS or sleep disturbances. These symptoms are the reason women seek therapeutic relief in menopause, however the enumeration of the symptoms is less common than a menopausal diagnosis. The purpose of this cohort is to explore treatment patterns for those experiencing the percise menopausal symptoms. The cohort definition is identical to cohort 1 except the index event is the menopausal symptom. 


**Version 2** 07/14/2023

- ignore observation period for hysterectomy and oophorectomy to ensure never took place anytime prior to menopause
- to account for the potential of multiple symptoms, open index to all possible events of a symptom instead of the earliest event. 

**Version 1** 

- define index event as menopausal symptom instead of menopause as an exploratory cohort


### Cohort 6: Women on HT prescription

**Description**: This cohort definition is meant to be used for exploratory analysis where the index event is the exposure to a hormone therapy. Want to view treatment patterns after the start of a hormone therapy


**Version 2** 07/14/2023

- ignore observation period for hysterectomy and oophorectomy to ensure never took place anytime prior to menopause
- to account for the potential of multiple symptoms, open index to all possible events of a symptom instead of the earliest event. 

## Outcomes

The general cohort skeleton is a simple prevalent condition cohort. 

* anxiety
* bipolar
* depression
* combination of anxiety, bipolar and depression
* IVMS
* organic sleep
* sleep disturbances
* vms

## Exposure cohorts

All exposure cohort follow the same skeletal logic:

- Index event: a drug exposure with at least 365 days of prior observation. Include all events
- Exit: based on persistence to the index drug exposure and censor for when patients turn 65 based on a visit occurrence
- Era: collapse drug eras based on 30 day gaps

### ATC Class 1

**Description**: These cohorts serve as the highest aggregation of drugs used to relieve symptoms of menopausal women. Class 1 contains groupings of several groupings of drugs by ATC class. This includes:

* anticonvulsants
* antidepressants
* antihypertensives
* benzodiazepines
* hormone therapy



### ATC Class 3

These cohorts serve as the 2nd highest aggregation of drugs used to relieve symptoms of menopausal women. Class 3 contains grouping of ingredients based on ATC codes. This includes:

* anticonvulsants
* antidepressants
* antihypertensives
* benzodiazepines
* hormone therapy

### Ingredient Level

These cohorts serve as the granular level of drugs used to relieve symptoms of menopausal women. Ingredients consider each drug as a single level for analysis. This includes:

* amitriptyline
* benzodiazepines
* citalopram
* clonidine
* desvenlafaxine
* escitalopram
* estrogens
* estrogens in combination
* fluoxetine
* gabapentin
* paroxetine
* pregabalin
* progestogens
* progestogens + estrogen
* raloxifene
* sertraline
* venlafaxine
