-- First TEMP TABLE creation-----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS OI;
CREATE TEMP TABLE OI AS
SELECT *
FROM (
    SELECT 
        bg.subject_id, 
        bg.hadm_id, 
        ventilation.stay_id,
        bg.charttime, 
        bg.pao2fio2ratio, 
        ventilator_setting.charttime AS ventilator_charttime, 
        ventilator_setting.peep,
        ventilation.starttime, 
        ventilation.endtime, 
        ventilation.ventilation_status,
        chexpert.edema,  
        metadata.studydate,
        ROW_NUMBER() OVER (PARTITION BY bg.subject_id ORDER BY bg.charttime ASC) AS ROWNUMBER
    FROM 
        mimiciv.mimiciv_derived.bg AS bg
    INNER JOIN
        mimiciv.mimiciv_derived.ventilator_setting AS ventilator_setting
        ON bg.subject_id = ventilator_setting.subject_id
    INNER JOIN
        mimiciv.mimiciv_derived.ventilation AS ventilation
        ON ventilator_setting.stay_id = ventilation.stay_id
    INNER JOIN
        mimiciv_cxr.chexpert AS chexpert
        ON bg.subject_id = chexpert.subject_id
    INNER JOIN
        mimiciv_cxr.metadata AS metadata
        ON chexpert.study_id = metadata.study_id
    WHERE 
        bg.pao2fio2ratio <= 300
        AND ventilation.ventilation_status = 'InvasiveVent'
        AND ventilator_setting.peep >= 5
        AND chexpert.edema = 1
        AND ventilation.starttime BETWEEN bg.charttime AND bg.charttime + INTERVAL '2 DAY'
        AND ventilator_setting.charttime BETWEEN bg.charttime AND bg.charttime + INTERVAL '2 DAY'
        AND TO_TIMESTAMP(metadata.studydate::TEXT, 'YYYYMMDD') BETWEEN CAST(bg.charttime AS DATE) AND CAST(bg.charttime AS DATE) + INTERVAL '2 DAY'
) AS subquery
WHERE ROWNUMBER = 1;



-- Second TEMP TABLE creation----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS NEWOI;
CREATE TEMP TABLE NEWOI AS
SELECT *
FROM (
    SELECT 
       vitalsign.subject_id, 
       vitalsign.stay_id AS vitalsign_stay_id, 
       vitalsign.charttime,  
       vitalsign.spo2,
       oxygen_delivery.charttime AS oxygen_delivery_charttime,  
       oxygen_delivery.o2_flow,
       ventilator_setting.charttime AS ventilator_setting_charttime,  
       ventilator_setting.peep,
       ventilation.stay_id AS ventilation_stay_id,  
       ventilation.starttime, 
       ventilation.endtime, 
       ventilation.ventilation_status,
       chexpert.edema, 
       metadata.studydate,
       ROW_NUMBER() OVER (PARTITION BY vitalsign.subject_id ORDER BY vitalsign.charttime ASC) AS ROWNUMBER
    FROM 
        mimiciv.mimiciv_derived.vitalsign AS vitalsign
    INNER JOIN
        mimiciv.mimiciv_derived.oxygen_delivery AS oxygen_delivery
        ON vitalsign.subject_id = oxygen_delivery.subject_id
    INNER JOIN
        mimiciv.mimiciv_derived.ventilator_setting AS ventilator_setting
        ON vitalsign.subject_id = ventilator_setting.subject_id
    INNER JOIN
        mimiciv.mimiciv_derived.ventilation AS ventilation
        ON ventilator_setting.stay_id = ventilation.stay_id
    INNER JOIN
        mimiciv_cxr.chexpert AS chexpert
        ON vitalsign.subject_id = chexpert.subject_id
    INNER JOIN
        mimiciv_cxr.metadata AS metadata
        ON chexpert.study_id = metadata.study_id
    WHERE 
        (100 * vitalsign.spo2) / (21 + 4 * oxygen_delivery.o2_flow) <= 315
        AND vitalsign.spo2 <= 97
        AND ventilation.ventilation_status = 'InvasiveVent'
        AND ventilator_setting.peep >= 5
        AND chexpert.edema = 1
         AND ventilation.starttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
        AND ventilator_setting.charttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
				AND oxygen_delivery.charttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
        AND TO_TIMESTAMP(metadata.studydate::TEXT, 'YYYYMMDD') BETWEEN CAST(vitalsign.charttime AS DATE) AND CAST(vitalsign.charttime AS DATE) + INTERVAL '2 DAY'
) AS subquery
WHERE ROWNUMBER = 1;


-- First part of the UNION (using the OI temp table)-----------------------------------------------------------------------------
DROP TABLE IF EXISTS INARDS1;
CREATE TEMP TABLE INARDS1 AS
SELECT 
    OI.subject_id,  
    OI.charttime 
FROM 
    OI
UNION ALL
SELECT 
    NEWOI.subject_id, 
    NEWOI.charttime
FROM 
    NEWOI;
		
--------------------------筛选唯一ID---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS INARDSFinal;
CREATE TEMP TABLE INARDSFinal AS
SELECT *
FROM (
SELECT subject_id, charttime, ROW_NUMBER() OVER(PARTITION BY INARDS1.subject_id ORDER BY charttime) AS ROWNUMBER
FROM INARDS1 
)
WHERE ROWNUMBER = 1
;









--------------------------非气管插管患者ARDS诊断数据提取,第一步是OI诊断的----------------------------------------------------------------
DROP TABLE IF EXISTS NOINOI2;
CREATE TEMP TABLE NOINOI2 AS
SELECT *
FROM (
    SELECT 
        bg.subject_id, 
        bg.charttime, 
        bg.pao2fio2ratio, 
        ventilator_setting.subject_id AS ventilator_subject_id, 
        ventilator_setting.stay_id, 
        ventilator_setting.charttime AS ventilator_charttime, 
        ventilator_setting.peep, 
        ventilator_setting.flow_rate, 
        ventilation.stay_id AS ventilation_stay_id, 
        ventilation.starttime, 
        ventilation.endtime, 
        ventilation.ventilation_status, 
        chexpert.edema,
        metadata.studydate,
        ROW_NUMBER() OVER (PARTITION BY bg.subject_id ORDER BY bg.charttime ASC) AS ROWNUMBER
    FROM mimiciv.mimiciv_derived.bg AS bg
    INNER JOIN mimiciv.mimiciv_derived.ventilator_setting AS ventilator_setting
        ON bg.subject_id = ventilator_setting.subject_id
    INNER JOIN mimiciv.mimiciv_derived.ventilation AS ventilation
        ON ventilator_setting.stay_id = ventilation.stay_id
    INNER JOIN mimiciv.mimiciv_cxr.chexpert AS chexpert
        ON bg.subject_id = chexpert.subject_id
    INNER JOIN mimiciv_cxr.metadata AS metadata
        ON chexpert.study_id = metadata.study_id
    WHERE bg.pao2fio2ratio <= 300
        AND (
            (ventilation.ventilation_status = 'NonInvasiveVent' AND ventilator_setting.peep >= 5)
            OR
            (ventilation.ventilation_status = 'HFNC' AND ventilator_setting.flow_rate >= 30) 
        )
        AND chexpert.edema = 1
        AND ventilator_setting.charttime BETWEEN bg.charttime AND bg.charttime + INTERVAL '2 DAY'
        AND ventilation.starttime BETWEEN bg.charttime AND bg.charttime + INTERVAL '2 DAY'
        AND TO_TIMESTAMP(metadata.studydate::TEXT, 'YYYYMMDD') BETWEEN CAST(bg.charttime AS DATE) AND CAST(bg.charttime AS DATE) + INTERVAL '2 DAY'
) 
WHERE ROWNUMBER = 1
;

-----------------------spo2fio2诊断---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS NOINspo2fio2;
CREATE TEMP TABLE NOINspo2fio2 AS
SELECT *
FROM (
    SELECT 
       vitalsign.subject_id, 
       vitalsign.stay_id AS vitalsign_stay_id, 
       vitalsign.charttime,  
       vitalsign.spo2,
       oxygen_delivery.charttime AS oxygen_delivery_charttime,  
       oxygen_delivery.o2_flow,
       ventilator_setting.charttime AS ventilator_setting_charttime,  
       ventilator_setting.peep,
       ventilation.stay_id AS ventilation_stay_id,  
       ventilation.starttime, 
       ventilation.endtime, 
       ventilation.ventilation_status,
       chexpert.edema, 
       metadata.studydate,
        ROW_NUMBER() OVER (PARTITION BY vitalsign.subject_id ORDER BY vitalsign.charttime ASC) AS ROWNUMBER
    FROM mimiciv.mimiciv_derived.vitalsign AS vitalsign
		
    INNER JOIN mimiciv.mimiciv_derived.oxygen_delivery AS oxygen_delivery
        ON vitalsign.subject_id = oxygen_delivery.subject_id
				
    INNER JOIN mimiciv.mimiciv_derived.ventilator_setting AS ventilator_setting
        ON ventilator_setting.subject_id = vitalsign.subject_id
				
    INNER JOIN mimiciv.mimiciv_derived.ventilation AS ventilation
        ON ventilation.stay_id = ventilator_setting.stay_id
		
		INNER JOIN mimiciv.mimiciv_cxr.chexpert AS chexpert
        ON vitalsign.subject_id = chexpert.subject_id
				
    INNER JOIN mimiciv_cxr.metadata AS metadata
        ON chexpert.study_id = metadata.study_id
				
    WHERE  
		    (100 * vitalsign.spo2) / (21 + 4 * oxygen_delivery.o2_flow) <= 315
        AND vitalsign.spo2 <= 97
        AND (
            (ventilation.ventilation_status = 'NonInvasiveVent' AND ventilator_setting.peep >= 5)
            OR
            (ventilation.ventilation_status = 'HFNC' AND ventilator_setting.flow_rate >= 30) 
        )
        AND chexpert.edema = 1
        AND ventilator_setting.charttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
				AND oxygen_delivery.charttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
        AND ventilation.starttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
        AND TO_TIMESTAMP(metadata.studydate::TEXT, 'YYYYMMDD') BETWEEN CAST(vitalsign.charttime AS DATE) AND CAST(vitalsign.charttime AS DATE) + INTERVAL '2 DAY'
) 
WHERE ROWNUMBER = 1
;
-----------------------------------------------非插管患者spo2fio2数据提取---资源有限---------------------------------------------
DROP TABLE IF EXISTS spo2fio2;
CREATE TEMP TABLE spo2fio2 AS
SELECT *
FROM (
SELECT vitalsign.subject_id, vitalsign.charttime, vitalsign.spo2,
       oxygen_delivery.charttime AS oxygen_deliverycharttime, oxygen_delivery.o2_flow,
			 chexpert.edema, 
       metadata.studydate,
			 ROW_NUMBER() OVER (PARTITION BY vitalsign.subject_id ORDER BY vitalsign.charttime ASC) AS ROWNUMBER
			 FROM mimiciv.mimiciv_derived.vitalsign AS vitalsign
			 
			 INNER JOIN 
			 mimiciv.mimiciv_derived.oxygen_delivery AS oxygen_delivery
			 ON vitalsign.subject_id = oxygen_delivery.subject_id
			 
			 INNER JOIN 
			 mimiciv.mimiciv_cxr.chexpert AS chexpert
			 ON vitalsign.subject_id = chexpert.subject_id
			 
			 INNER JOIN mimiciv.mimiciv_cxr.metadata AS metadata
			 ON chexpert.study_id = metadata.study_id
			 
			 WHERE (100 * vitalsign.spo2) / (21 + 4 * oxygen_delivery.o2_flow) <= 315
        AND vitalsign.spo2 <= 97
				AND chexpert.edema = 1
        AND oxygen_delivery.charttime BETWEEN vitalsign.charttime AND vitalsign.charttime + INTERVAL '2 DAY'
        AND TO_TIMESTAMP(metadata.studydate::TEXT, 'YYYYMMDD') BETWEEN CAST(vitalsign.charttime AS DATE) AND CAST(vitalsign.charttime AS DATE) + INTERVAL '2 DAY'
			  AND CAST(vitalsign.charttime AS DATE) = CAST(oxygen_delivery.charttime AS DATE) 
)
WHERE ROWNUMBER = 1
;
------------------------总提取2023新定义ARDS患者---------------------------------------
DROP TABLE IF EXISTS djtardsss;
CREATE TEMP TABLE djtardsss AS
SELECT 
    OI.subject_id,  
    OI.charttime 
FROM 
    OI
UNION ALL
SELECT 
    NEWOI.subject_id, 
    NEWOI.charttime
FROM 
    NEWOI
		
UNION ALL
SELECT 
    NOINOI2.subject_id, 
    NOINOI2.charttime
FROM 
    NOINOI2
		
UNION ALL
SELECT 
    NOINspo2fio2.subject_id, 
    NOINspo2fio2.charttime
FROM 
    NOINspo2fio2	

UNION ALL
SELECT 
    spo2fio2.subject_id, 
    spo2fio2.charttime
FROM 
    spo2fio2			
;

--心源性休克患者
DROP TABLE IF EXISTS xyxxk;
CREATE TEMP TABLE xyxxk AS
SELECT
    adm.subject_id,
    adm.hadm_id,
    adm.admittime,
    adm.dischtime,
    dname.long_title
FROM mimiciv_hosp.admissions AS adm
INNER JOIN mimiciv_hosp.diagnoses_icd AS dx
    ON adm.hadm_id = dx.hadm_id
INNER JOIN mimiciv_hosp.d_icd_diagnoses AS dname
    ON dname.icd_code = dx.icd_code
WHERE
    LOWER(dname.long_title) LIKE '%acute%'
    AND LOWER(dname.long_title) LIKE '%heart failure%'
ORDER BY adm.subject_id, adm.admittime, dname.long_title;

--此次住院心源性休克患者
DROP TABLE IF EXISTS exclude_ids2;
CREATE TEMP TABLE exclude_ids2 AS
SELECT DISTINCT d.subject_id
FROM djtardsss d
LEFT JOIN xyxxk x
  ON d.subject_id = x.subject_id
 WHERE d.charttime BETWEEN x.admittime AND x.dischtime;


--排除心源性性休克患者
DROP TABLE IF EXISTS djtards;
CREATE TEMP TABLE djtards AS
SELECT d.subject_id, d.charttime
FROM djtardsss d
LEFT JOIN exclude_ids2 x ON d.subject_id = x.subject_id
WHERE x.subject_id IS NULL
;

-------筛查ARDS患者合并谵妄----join就是inner join-----------------------------------------------------------------
 DROP TABLE IF EXISTS ardsdeliriumraw;
 CREATE TEMP TABLE ardsdeliriumraw AS
 SELECT 
        djtards.subject_id, 
        chartevents.stay_id, 
        djtards.charttime, 
        d_items.label, 
        chartevents.value,
				chartevents.charttime AS dtime
    FROM mimiciv.mimiciv_icu.chartevents
    JOIN mimiciv.mimiciv_icu.d_items 
      ON chartevents.itemid = d_items.itemid
    JOIN djtards 
      ON djtards.subject_id = chartevents.subject_id  
    WHERE LOWER(d_items.label) LIKE '%delirium%'
      AND chartevents.charttime BETWEEN djtards.charttime AND djtards.charttime + INTERVAL '7 DAY'   
 ;     
      

      
---转换成数字进行优先级的选择djtdeliriumardsicu
DROP TABLE IF EXISTS djtdeliriumardsicu;
CREATE TEMP TABLE djtdeliriumardsicu AS
SELECT subject_id, stay_id, charttime, label, value, dtime
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY subject_id
               ORDER BY 
                   -- 优先级顺序：Positive (1) < Negative (2) < UTA (3)
                   CASE value
                       WHEN 'Positive' THEN 1
                       WHEN 'Negative' THEN 2
                       WHEN 'UTA' THEN 3
                       ELSE 4 -- 万一有其他异常值
                   END,
                   charttime  -- 同一类型内按时间排序。这里的，是合并的意思，合并charttime
           ) AS rn
    FROM ardsdeliriumraw  
) AS ranked
WHERE rn = 1;




----筛查实验室指标
DROP TABLE IF EXISTS labevents;
CREATE TEMP TABLE labevents AS
SELECT djtdeliriumardsicu.*,
       d_labitems.itemid,
       d_labitems.label AS dlabel,
       labevents.valuenum
FROM djtdeliriumardsicu
LEFT JOIN mimiciv_hosp.labevents
ON djtdeliriumardsicu.subject_id = labevents.subject_id
LEFT JOIN mimiciv_hosp.d_labitems
ON labevents.itemid = d_labitems.itemid
WHERE labevents.charttime BETWEEN djtdeliriumardsicu.charttime AND djtdeliriumardsicu.charttime + INTERVAL '24 hours'
;


----筛查实验室指标,每个人每种检验的最大值
DROP TABLE IF EXISTS labevents1;
CREATE TEMP TABLE labevents1 AS
SELECT labevents.subject_id,
       labevents.dlabel,
       MAX(labevents.valuenum)
FROM labevents
GROUP BY labevents.dlabel, labevents.subject_id
;




---筛查实验室检查
DROP TABLE IF EXISTS labeventss;
CREATE TEMP TABLE labeventss AS
SELECT  
  subject_id,
  MAX(CASE WHEN dlabel = 'Glucose' THEN valuenum END) AS glucose,
  MAX(CASE WHEN dlabel = 'Sodium' THEN valuenum END) AS sodium,
  MAX(CASE WHEN dlabel = 'Chloride' THEN valuenum END) AS chloride,
  MAX(CASE WHEN dlabel = 'Potassium' THEN valuenum END) AS potassium,
  MAX(CASE WHEN dlabel = 'Creatinine' THEN valuenum END) AS creatinine,
  MAX(CASE WHEN dlabel = 'Urea Nitrogen' THEN valuenum END) AS urea_nitrogen,
  MAX(CASE WHEN dlabel = 'Anion Gap' THEN valuenum END) AS anion_gap,
  MAX(CASE WHEN dlabel = 'Bicarbonate' THEN valuenum END) AS bicarbonate,
  MAX(CASE WHEN dlabel = 'Hematocrit' THEN valuenum END) AS hematocrit,
  MAX(CASE WHEN dlabel = 'Hemoglobin' THEN valuenum END) AS hemoglobin,
  MAX(CASE WHEN dlabel = 'Platelet Count' THEN valuenum END) AS platelet_count,
  MAX(CASE WHEN dlabel = 'White Blood Cells' THEN valuenum END) AS white_blood_cells,
  MAX(CASE WHEN dlabel = 'Red Blood Cells' THEN valuenum END) AS red_blood_cells,
  MAX(CASE WHEN dlabel = 'MCH' THEN valuenum END) AS mch,
  MAX(CASE WHEN dlabel = 'RDW' THEN valuenum END) AS rdw,
  MAX(CASE WHEN dlabel = 'MCV' THEN valuenum END) AS mcv,
  MAX(CASE WHEN dlabel = 'MCHC' THEN valuenum END) AS mchc,
  MAX(CASE WHEN dlabel = 'Magnesium' THEN valuenum END) AS magnesium,
  MAX(CASE WHEN dlabel = 'Phosphate' THEN valuenum END) AS phosphate,
  MAX(CASE WHEN dlabel = 'Calcium, Total' THEN valuenum END) AS calcium_total,
  MAX(CASE WHEN dlabel = 'INR(PT)' THEN valuenum END) AS inr_pt,
  MAX(CASE WHEN dlabel = 'PT' THEN valuenum END) AS pt,
  MAX(CASE WHEN dlabel = 'PTT' THEN valuenum END) AS ptt
FROM labevents
GROUP BY subject_id;




--合并实验室检查结果
DROP TABLE IF EXISTS labeventssed;
CREATE TEMP TABLE labeventssed AS
SELECT 
  djtdeliriumardsicu.*,
  labeventss.glucose,
  labeventss.sodium,
  labeventss.potassium,
  labeventss.chloride,
  labeventss.creatinine,
  labeventss.urea_nitrogen,
  labeventss.anion_gap,
  labeventss.bicarbonate,
  labeventss.hematocrit,
  labeventss.hemoglobin,
  labeventss.platelet_count,
  labeventss.white_blood_cells,
  labeventss.red_blood_cells,
  labeventss.mch,
  labeventss.rdw,
  labeventss.mcv,
  labeventss.mchc,
  labeventss.magnesium,
  labeventss.phosphate,
  labeventss.calcium_total,
  labeventss.inr_pt,
  labeventss.pt,
  labeventss.ptt
FROM djtdeliriumardsicu
LEFT JOIN labeventss
  ON djtdeliriumardsicu.subject_id = labeventss.subject_id;
  
  
  
---药物筛查
DROP TABLE IF EXISTS drugs;
CREATE TEMP TABLE drugs AS
SELECT djtdeliriumardsicu.*,
       prescriptions.drug,
       prescriptions.dose_val_rx
FROM djtdeliriumardsicu
LEFT JOIN mimiciv_hosp.prescriptions
ON djtdeliriumardsicu.subject_id = prescriptions.subject_id
AND prescriptions.starttime BETWEEN djtdeliriumardsicu.charttime AND djtdeliriumardsicu.charttime + INTERVAL '24 hours'
;



---药物筛查，每个人每种药物
DROP TABLE IF EXISTS drugs3;
CREATE TEMP TABLE drugs3 AS
SELECT 
  drugs.subject_id,
  -- Fluids
MAX(CASE 
  WHEN drugs.drug LIKE '%Lactated Ringers%' 
    OR drugs.drug LIKE '%Iso-Osmotic Dextrose%' 
    OR drugs.drug LIKE '%0.9% Sodium Chloride%' 
    OR drugs.drug LIKE '%5% Dextrose%' 
    OR drugs.drug LIKE '%D5W%' 
    OR drugs.drug LIKE '%D5NS%' 
    OR drugs.drug LIKE '%D5 1/2NS%' 
    OR drugs.drug LIKE '%D5LR%' 
    THEN 1 ELSE 0 
END) AS fluids,
  -- Analgesics and Sedatives
MAX(CASE 
  WHEN drugs.drug LIKE '%Fentanyl Citrate%' 
    OR drugs.drug LIKE '%HYDROmorphone%' 
    OR drugs.drug LIKE '%Propofol%' 
    OR drugs.drug LIKE '%Midazolam%' 
    OR drugs.drug LIKE '%Lorazepam%' 
    OR drugs.drug LIKE '%Morphine Sulfate%' 
    OR drugs.drug LIKE '%Dexmedetomidine%' 
    OR drugs.drug LIKE '%Ketamine%' 
    OR drugs.drug LIKE '%Oxycodone%' 
    OR drugs.drug LIKE '%TraMADol%'
    THEN 1 ELSE 0 
END) AS analgesics_sedatives,
  -- Laxatives
MAX(CASE 
  WHEN drugs.drug LIKE '%Senna%' 
    OR drugs.drug LIKE '%Docusate Sodium%' 
    OR drugs.drug LIKE '%Bisacodyl%' 
    OR drugs.drug LIKE '%Lactulose%' 
    OR drugs.drug LIKE '%Polyethylene Glycol%' 
    OR drugs.drug LIKE '%Milk of Magnesia%' 
    OR drugs.drug LIKE '%Sodium Phosphate%' 
    OR drugs.drug LIKE '%Fleet Enema%' 
    THEN 1 ELSE 0 
END) AS laxatives,
  -- Vasopressors
MAX(CASE 
  WHEN drugs.drug LIKE '%Norepinephrine%' 
    OR drugs.drug LIKE '%PHENYLEPHrine%' 
    OR drugs.drug LIKE '%Dopamine%' 
    OR drugs.drug LIKE '%Vasopressin%' 
    OR drugs.drug LIKE '%Dobutamine%' 
    OR drugs.drug LIKE '%Epinephrine%' 
    THEN 1 ELSE 0 
END) AS vasopressors,
  -- Anticoagulants
MAX(CASE 
  WHEN drugs.drug LIKE '%Heparin%' 
    OR drugs.drug LIKE '%Heparin Sodium%'
    OR drugs.drug LIKE '%Enoxaparin%' 
    OR drugs.drug LIKE '%Warfarin%' 
    OR drugs.drug LIKE '%Rivaroxaban%' 
    OR drugs.drug LIKE '%Apixaban%' 
    OR drugs.drug LIKE '%Dabigatran%' 
    THEN 1 ELSE 0 
END) AS anticoagulants,
  -- Antibiotics
  MAX(CASE 
    WHEN drugs.drug LIKE '%Amoxicillin-Clavulanic Acid%'
      OR drugs.drug LIKE '%Ampicillin%'
      OR drugs.drug LIKE '%Ampicillin-Sulbactam%'
      OR drugs.drug LIKE '%Azithromycin%'
      OR drugs.drug LIKE '%Aztreonam%'
      OR drugs.drug LIKE '%Cefazolin%'
      OR drugs.drug LIKE '%Cefepime%'
      OR drugs.drug LIKE '%CefTAZidime%'
      OR drugs.drug LIKE '%CefTRIAXone%'
      OR drugs.drug LIKE '%Ciprofloxacin%'
      OR drugs.drug LIKE '%Clindamycin%'
      OR drugs.drug LIKE '%Daptomycin%'
      OR drugs.drug LIKE '%Doxycycline%'
      OR drugs.drug LIKE '%Gentamicin%'
      OR drugs.drug LIKE '%Imipenem-Cilastatin%'
      OR drugs.drug LIKE '%Levofloxacin%'
      OR drugs.drug LIKE '%Linezolid%'
      OR drugs.drug LIKE '%Meropenem%'
      OR drugs.drug LIKE '%Metronidazole%'
      OR drugs.drug LIKE '%Nafcillin%'
      OR drugs.drug LIKE '%Sulfamethoxazole-Trimethoprim%'
      OR drugs.drug LIKE '%Trimethoprim%'
      OR drugs.drug LIKE '%Tobramycin%'
      OR drugs.drug LIKE '%Vancomycin%'
      THEN 1 ELSE 0 
  END) AS antibiotics,
  -- Acid suppressants
MAX(CASE 
  WHEN drugs.drug LIKE '%Pantoprazole%' 
    OR drugs.drug LIKE '%Famotidine%' 
    OR drugs.drug LIKE '%Omeprazole%' 
    OR drugs.drug LIKE '%Ranitidine%' 
    OR drugs.drug LIKE '%Lansoprazole%' 
    THEN 1 ELSE 0 
END) AS acid_suppressants,
  -- Statins
  MAX(CASE 
    WHEN drugs.drug LIKE '%Atorvastatin%'    -- 阿托伐他汀
      OR drugs.drug LIKE '%Rosuvastatin%'    -- 瑞舒伐他汀
      OR drugs.drug LIKE '%Simvastatin%'     -- 辛伐他汀
      OR drugs.drug LIKE '%Pravastatin%'     -- 普伐他汀
      THEN 1 ELSE 0 
  END) AS statins,
  -- Antiplatelets
MAX(CASE 
  WHEN drugs.drug LIKE '%Aspirin%' 
    OR drugs.drug LIKE '%Clopidogrel%' 
    OR drugs.drug LIKE '%Ticagrelor%' 
    THEN 1 ELSE 0 
END) AS antiplatelets,
  -- Steroids
  MAX(CASE 
    WHEN drugs.drug LIKE '%Methylprednisolone%' 
      OR drugs.drug LIKE '%Prednisone%' 
      OR drugs.drug LIKE '%Hydrocortisone%' 
      OR drugs.drug LIKE '%Dexamethasone%' 
      THEN 1 ELSE 0 
  END) AS steroids,
  -- Antipsychotics
MAX(CASE 
  WHEN drugs.drug LIKE '%Haloperidol%' 
    OR drugs.drug LIKE '%Olanzapine%' 
    OR drugs.drug LIKE '%Quetiapine%' 
    OR drugs.drug LIKE '%Risperidone%' 
    OR drugs.drug LIKE '%Ziprasidone%' 
    OR drugs.drug LIKE '%Aripiprazole%' 
    OR drugs.drug LIKE '%Clozapine%' 
    OR drugs.drug LIKE '%Chlorpromazine%' 
    OR drugs.drug LIKE '%Perphenazine%' 
    OR drugs.drug LIKE '%Thioridazine%' 
    THEN 1 ELSE 0 
END) AS antipsychotics,
  -- Antiepileptics
MAX(CASE 
  WHEN drugs.drug LIKE '%Levetiracetam%' 
    OR drugs.drug LIKE '%Valproic Acid%' 
    OR drugs.drug LIKE '%Divalproex%' 
    OR drugs.drug LIKE '%Phenytoin%' 
    OR drugs.drug LIKE '%Phenobarbital%' 
    OR drugs.drug LIKE '%Lamotrigine%' 
    OR drugs.drug LIKE '%Oxcarbazepine%' 
    OR drugs.drug LIKE '%Carbamazepine%' 
    OR drugs.drug LIKE '%Lacosamide%' 
    OR drugs.drug LIKE '%Gabapentin%' 
    OR drugs.drug LIKE '%Topiramate%' 
    THEN 1 ELSE 0 
END) AS antiepileptics,
-- Beta Blockers
MAX(CASE 
  WHEN drugs.drug LIKE '%Metoprolol%'
    OR drugs.drug LIKE '%Labetalol%'
    OR drugs.drug LIKE '%Carvedilol%'
    OR drugs.drug LIKE '%Atenolol%'
    OR drugs.drug LIKE '%Esmolol%'
    OR drugs.drug LIKE '%Propranolol%'
    THEN 1 ELSE 0 
END) AS beta_blockers,
-- RAAS Inhibitors
MAX(CASE 
  WHEN drugs.drug LIKE '%Lisinopril%'
    OR drugs.drug LIKE '%Captopril%'
    OR drugs.drug LIKE '%Enalapril%'
    OR drugs.drug LIKE '%Ramipril%'
    OR drugs.drug LIKE '%Losartan%'
    OR drugs.drug LIKE '%Valsartan%'
    OR drugs.drug LIKE '%Irbesartan%'
    OR drugs.drug LIKE '%Telmisartan%'
    OR drugs.drug LIKE '%Olmesartan%'
    THEN 1 ELSE 0 
END) AS raas_inhibitors
FROM drugs
GROUP BY drugs.subject_id;



---药物筛查，每个人每种药物
DROP TABLE IF EXISTS drugslabventsed;
CREATE TEMP TABLE drugslabventsed AS
SELECT 
  labeventssed.*,
  drugs3.fluids,
  drugs3.analgesics_sedatives,
  drugs3.laxatives,
  drugs3.vasopressors,
  drugs3.anticoagulants,
  drugs3.antibiotics,
  drugs3.acid_suppressants,
  drugs3.statins,
  drugs3.antiplatelets,
  drugs3.steroids,
  drugs3.antipsychotics,
  drugs3.antiepileptics,
  drugs3.beta_blockers,
  drugs3.raas_inhibitors
FROM labeventssed
LEFT JOIN drugs3
  ON labeventssed.subject_id = drugs3.subject_id;




---合并症筛查
DROP TABLE IF EXISTS comed;
CREATE TEMP TABLE comed AS
SELECT 
  drugslabventsed.subject_id,
  drugslabventsed.stay_id,
  drugslabventsed.charttime,
  icustay_detail.hadm_id,
	icustay_detail.gender,
	icustay_detail.admission_age,
	icustay_detail.dod,
	icustay_detail.los_icu,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '410%' OR diagnoses_icd.icd_code LIKE 'I21%' 
    THEN 1 ELSE 0 END) AS myocardial_infarction,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '428%' OR diagnoses_icd.icd_code LIKE 'I50%' 
    THEN 1 ELSE 0 END) AS congestive_heart_failure,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '250%' 
      OR diagnoses_icd.icd_code LIKE 'E10%' 
      OR diagnoses_icd.icd_code LIKE 'E11%' 
    THEN 1 ELSE 0 END) AS diabetes,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '585%' OR diagnoses_icd.icd_code LIKE 'N18%' 
    THEN 1 ELSE 0 END) AS chronic_kidney_disease,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '491%' 
      OR diagnoses_icd.icd_code LIKE 'J44%' 
      OR diagnoses_icd.icd_code LIKE 'J43%' 
    THEN 1 ELSE 0 END) AS chronic_obstructive_pulmonary_disease,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '140%' OR diagnoses_icd.icd_code LIKE 'C%' 
    THEN 1 ELSE 0 END) AS malignancy,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '401%' OR diagnoses_icd.icd_code LIKE 'I10%' 
    THEN 1 ELSE 0 END) AS hypertension,
  MAX(CASE 
    WHEN diagnoses_icd.icd_code LIKE '295%' 
      OR diagnoses_icd.icd_code LIKE '296%' 
      OR diagnoses_icd.icd_code LIKE '300%' 
      OR diagnoses_icd.icd_code LIKE '311%' 
      OR diagnoses_icd.icd_code LIKE '290%' 
      OR diagnoses_icd.icd_code LIKE 'F%' 
    THEN 1 ELSE 0 
  END) AS psychiatric_disorder
FROM drugslabventsed
LEFT JOIN mimiciv_derived.icustay_detail
  ON drugslabventsed.stay_id = mimiciv_derived.icustay_detail.stay_id
LEFT JOIN mimiciv_hosp.diagnoses_icd
  ON mimiciv_derived.icustay_detail.hadm_id = mimiciv_hosp.diagnoses_icd.hadm_id
LEFT JOIN mimiciv_hosp.admissions
  ON mimiciv_derived.icustay_detail.hadm_id = mimiciv_hosp.admissions.hadm_id
  AND mimiciv_hosp.admissions.admittime <= drugslabventsed.charttime
GROUP BY 
  drugslabventsed.subject_id,
  drugslabventsed.stay_id,
  drugslabventsed.charttime,
  icustay_detail.admission_age,
	icustay_detail.hadm_id,
	icustay_detail.dod,
	icustay_detail.los_icu,
	icustay_detail.gender;



------加入合并症
DROP TABLE IF EXISTS drugslabventscomed;
CREATE TEMP TABLE drugslabventscomed AS
SELECT 
  drugslabventsed.*,
  comed.hadm_id,
  comed.gender,
  comed.admission_age,
  comed.dod,
  comed.los_icu,								 
  comed.myocardial_infarction,
  comed.congestive_heart_failure,
  comed.diabetes,
  comed.chronic_kidney_disease,
  comed.chronic_obstructive_pulmonary_disease,
  comed.malignancy,
  comed.hypertension,
  comed.psychiatric_disorder           -- 新增
FROM drugslabventsed
LEFT JOIN comed
  ON drugslabventsed.subject_id = comed.subject_id
  AND drugslabventsed.stay_id = comed.stay_id
  AND drugslabventsed.charttime = comed.charttime;

-- 医疗史
-- 步骤1：创建临时表存储当前住院信息
DROP TABLE IF EXISTS medicalhistory;
CREATE TEMP TABLE medicalhistory AS
SELECT 
    drugslabventscomed.*,
    admissions.admittime AS current_admittime  -- 当前住院时间
FROM drugslabventscomed
LEFT JOIN mimiciv_hosp.admissions
    ON drugslabventscomed.hadm_id = admissions.hadm_id;

-- 步骤2：筛查既往住院史
DROP TABLE IF EXISTS medicalhistory2;
CREATE TEMP TABLE medicalhistory2 AS
SELECT 
    medicalhistory.subject_id,
    medicalhistory.hadm_id,
    -- 如果存在比当前住院更早的记录，标记为1，否则0
    MAX(CASE WHEN prev_adm.admittime < medicalhistory.current_admittime THEN 1 ELSE 0 END) AS has_prior_admission
FROM medicalhistory
LEFT JOIN mimiciv_hosp.admissions prev_adm
    ON medicalhistory.subject_id = prev_adm.subject_id             -- 同一患者
    AND prev_adm.admittime < medicalhistory.current_admittime     -- 关键条件：仅统计早于当前住院的记录
GROUP BY medicalhistory.subject_id, medicalhistory.hadm_id;         -- 按患者+住院事件分组

-- 创伤史
DROP TABLE IF EXISTS traumaa;
CREATE TEMP TABLE traumaa AS
SELECT 
    medicalhistory.subject_id,
    MAX(
        CASE 
            WHEN diagnoses_icd.icd_code BETWEEN '800' AND '959'
             AND admissions.admittime <= medicalhistory.current_admittime
            THEN 1 ELSE 0
        END
    ) AS has_trauma_history
FROM medicalhistory
LEFT JOIN mimiciv_hosp.admissions
    ON medicalhistory.subject_id = admissions.subject_id
LEFT JOIN mimiciv_hosp.diagnoses_icd
    ON admissions.hadm_id = diagnoses_icd.hadm_id
GROUP BY medicalhistory.subject_id;

-- 酒精滥用史
DROP TABLE IF EXISTS alcoholica;
CREATE TEMP TABLE alcoholica AS
SELECT 
    medicalhistory.subject_id,
    MAX(
        CASE 
            WHEN (
                diagnoses_icd.icd_code LIKE '303%' 
                OR diagnoses_icd.icd_code LIKE '3050%'
                OR diagnoses_icd.icd_code LIKE '291%'
            )
            AND admissions.admittime <= medicalhistory.current_admittime
            THEN 1 ELSE 0
        END
    ) AS has_alcohol_abuse_history
FROM medicalhistory
LEFT JOIN mimiciv_hosp.admissions
    ON medicalhistory.subject_id = admissions.subject_id
LEFT JOIN mimiciv_hosp.diagnoses_icd
    ON admissions.hadm_id = diagnoses_icd.hadm_id
GROUP BY medicalhistory.subject_id;

-- 急诊入院
DROP TABLE IF EXISTS emergencea;
CREATE TEMP TABLE emergencea AS
SELECT 
    medicalhistory.subject_id,
    CASE 
        WHEN admissions.admission_type IN (
            'DIRECT EMER.',
            'EU OBSERVATION',
            'EW EMER.',
            'OBSERVATION ADMIT',
            'URGENT'
        )
        THEN 1 ELSE 0
    END AS is_emergency_admit
FROM medicalhistory
LEFT JOIN mimiciv_hosp.admissions
    ON medicalhistory.hadm_id = admissions.hadm_id;

-- BUN值（入院后24小时内）
DROP TABLE IF EXISTS buna;
CREATE TEMP TABLE buna AS
SELECT 
    medicalhistory.subject_id,
    MAX(labevents.valuenum) AS bun
FROM medicalhistory
LEFT JOIN mimiciv_hosp.labevents
    ON medicalhistory.subject_id = labevents.subject_id
    AND labevents.itemid = 51006  -- BUN的标准itemid
    WHERE labevents.charttime BETWEEN medicalhistory.charttime 
                               AND medicalhistory.current_admittime + INTERVAL '48 hours'
   
GROUP BY medicalhistory.subject_id;





-- 最终查询
DROP TABLE IF EXISTS laste;
CREATE TEMP TABLE laste AS
SELECT 
    drugslabventscomed.*,
    medicalhistory2.has_prior_admission,
    traumaa.has_trauma_history,
    alcoholica.has_alcohol_abuse_history,  
    emergencea.is_emergency_admit,
    buna.bun
FROM drugslabventscomed
LEFT JOIN medicalhistory2 ON drugslabventscomed.subject_id = medicalhistory2.subject_id
LEFT JOIN traumaa ON drugslabventscomed.subject_id = traumaa.subject_id
LEFT JOIN alcoholica ON drugslabventscomed.subject_id = alcoholica.subject_id
LEFT JOIN emergencea ON drugslabventscomed.subject_id = emergencea.subject_id
LEFT JOIN buna ON drugslabventscomed.subject_id = buna.subject_id;




-- 体温
DROP TABLE IF EXISTS tempraturea;
CREATE TEMP TABLE tempraturea AS
SELECT laste.subject_id, 
       MAX(chartevents.valuenum) AS temprature
FROM laste
LEFT JOIN mimiciv_icu.chartevents
    ON laste.subject_id = chartevents.subject_id
LEFT JOIN mimiciv_icu.d_items
    ON d_items.itemid = chartevents.itemid
WHERE 
    d_items.label ILIKE 'Temperature Fahrenheit'
    AND chartevents.charttime BETWEEN laste.charttime AND (laste.dtime + INTERVAL '24 hours')
GROUP BY laste.subject_id;



-- 呼吸频率
DROP TABLE IF EXISTS respiratory_rate;
CREATE TEMP TABLE respiratory_rate AS
SELECT 
    laste.subject_id,
    MAX(chartevents.valuenum) AS respiratory_rate
FROM laste
LEFT JOIN mimiciv_icu.chartevents
    ON laste.subject_id = chartevents.subject_id
    AND chartevents.charttime BETWEEN laste.charttime AND (laste.dtime + INTERVAL '24 hours')
LEFT JOIN mimiciv_icu.d_items
    ON d_items.itemid = chartevents.itemid
WHERE 
    d_items.label ILIKE 'Respiratory Rate'
GROUP BY laste.subject_id;



-- 心率
DROP TABLE IF EXISTS heart_rate_alarm_high;
CREATE TEMP TABLE heart_rate_alarm_high AS
SELECT 
    laste.subject_id,
    laste.stay_id,
    MAX(chartevents.valuenum) AS heart_rate
FROM laste
LEFT JOIN mimiciv_icu.chartevents
    ON laste.subject_id = chartevents.subject_id
    AND chartevents.charttime BETWEEN laste.charttime AND (laste.dtime + INTERVAL '24 hours')
LEFT JOIN mimiciv_icu.d_items
    ON d_items.itemid = chartevents.itemid
WHERE 
    d_items.label ILIKE 'Heart rate Alarm - High'
GROUP BY 
    laste.subject_id,
    laste.stay_id;



-- 无创血压均值
DROP TABLE IF EXISTS non_invasive_bp_mean;
CREATE TEMP TABLE non_invasive_bp_mean AS
SELECT 
    laste.subject_id,
    laste.stay_id,
    MAX(chartevents.valuenum) AS map
FROM laste
LEFT JOIN mimiciv_icu.chartevents
    ON laste.subject_id = chartevents.subject_id
    AND chartevents.charttime BETWEEN laste.charttime AND (laste.dtime + INTERVAL '24 hours')
LEFT JOIN mimiciv_icu.d_items
    ON d_items.itemid = chartevents.itemid
WHERE 
    d_items.label ILIKE '%Non Invasive Blood Pressure mean%'
GROUP BY 
    laste.subject_id,
    laste.stay_id;



-- 合并
DROP TABLE IF EXISTS hebing;
CREATE TEMP TABLE hebing AS
SELECT 
    laste.*,
    tempraturea.temprature,
    respiratory_rate.respiratory_rate,
    heart_rate_alarm_high.heart_rate,
    non_invasive_bp_mean.map
FROM laste
LEFT JOIN tempraturea
    ON laste.subject_id = tempraturea.subject_id
LEFT JOIN respiratory_rate
    ON laste.subject_id = respiratory_rate.subject_id
LEFT JOIN heart_rate_alarm_high
    ON laste.subject_id = heart_rate_alarm_high.subject_id
    AND laste.stay_id = heart_rate_alarm_high.stay_id
LEFT JOIN non_invasive_bp_mean
    ON laste.subject_id = non_invasive_bp_mean.subject_id
    AND laste.stay_id = non_invasive_bp_mean.stay_id;
		
		

-- SOFA评分
DROP TABLE IF EXISTS sofaa;
CREATE TEMP TABLE sofaa AS
SELECT 
    hebing.subject_id,
    MAX(sofa.sofa_24hours) AS sofa_score  -- 使用更明确的列名
FROM hebing
LEFT JOIN mimiciv_derived.sofa
    ON hebing.stay_id = sofa.stay_id
    AND sofa.starttime BETWEEN hebing.charttime AND (hebing.dtime + INTERVAL '24 hours')
GROUP BY hebing.subject_id;

-- GCS评分
DROP TABLE IF EXISTS gcsa;
CREATE TEMP TABLE gcsa AS
SELECT 
    hebing.subject_id,
    MIN(gcs.gcs) AS gcs_score  
FROM hebing
LEFT JOIN mimiciv_derived.gcs
    ON hebing.stay_id = gcs.stay_id
    AND gcs.charttime BETWEEN hebing.charttime AND (hebing.dtime + INTERVAL '24 hours')
GROUP BY hebing.subject_id;

-- APSIII评分
DROP TABLE IF EXISTS apsiiia;
CREATE TEMP TABLE apsiiia AS
SELECT 
    hebing.subject_id,
    MAX(apsiii.apsiii) AS apsiii_score  
FROM hebing
LEFT JOIN mimiciv_derived.apsiii
    ON hebing.stay_id = apsiii.stay_id
GROUP BY hebing.subject_id;

-- 最终合并
DROP TABLE IF EXISTS final_result;  
CREATE TEMP TABLE final_result AS
SELECT 
    hebing.*,
    sofaa.sofa_score AS sofa,
    gcsa.gcs_score AS gcs,
    apsiiia.apsiii_score AS apsiii
FROM hebing
LEFT JOIN sofaa
    ON hebing.subject_id = sofaa.subject_id
LEFT JOIN gcsa
    ON hebing.subject_id = gcsa.subject_id
LEFT JOIN apsiiia
    ON hebing.subject_id = apsiiia.subject_id;
		
		
	
	
-- 呼吸机筛查
DROP TABLE IF EXISTS ventilationeded;  
CREATE TEMP TABLE ventilationeded AS		
SELECT 
  final_result.subject_id,
  MAX(
    CASE 
      WHEN ventilation_status LIKE 'InvasiveVent'
           OR ventilation_status LIKE 'NonInvasiveVent' 
      THEN 1 ELSE 0 
    END
  ) AS ventilation
FROM final_result
LEFT JOIN mimiciv_derived.ventilation
  ON final_result.stay_id = ventilation.stay_id
	AND ventilation.starttime BETWEEN final_result.charttime AND final_result.charttime + INTERVAL '24 hours'
GROUP BY final_result.subject_id;






DROP TABLE IF EXISTS hfzj;  
CREATE TEMP TABLE hfzj AS		
SELECT final_result.*, 
       ventilationeded.ventilation
FROM final_result
LEFT JOIN ventilationeded
  ON final_result.subject_id = ventilationeded.subject_id
;




DROP TABLE IF EXISTS hfzj1;  
CREATE TEMP TABLE hfzj1 AS
WITH 
icu_time AS (
  SELECT stay_id, hadm_id, intime
  FROM mimiciv_icu.icustays
),

-- 仅统计ICU入科前30天内的手术
surgery_flag AS (
  SELECT 
    icu.hadm_id, 
    icu.stay_id, 
    1 AS has_surgery
  FROM icu_time icu
  INNER JOIN mimiciv_hosp.procedures_icd proc
    ON icu.hadm_id = proc.hadm_id
    AND proc.chartdate <= icu.intime::date
    AND proc.chartdate >= (icu.intime::date - INTERVAL '30 days')
  GROUP BY icu.hadm_id, icu.stay_id
),

-- 创伤诊断标记
trauma_flag AS (
  SELECT 
    icu.hadm_id, 
    icu.stay_id, 
    1 AS is_trauma
  FROM icu_time icu
  INNER JOIN mimiciv_hosp.diagnoses_icd diag
    ON icu.hadm_id = diag.hadm_id
  WHERE diag.icd_code LIKE 'S%' OR diag.icd_code LIKE 'T%'
  GROUP BY icu.hadm_id, icu.stay_id
),

-- 神经/神外诊断标记
neuro_diag_flag AS (
  SELECT
    icu.hadm_id,
    icu.stay_id,
    1 AS is_neuro
  FROM icu_time icu
  INNER JOIN mimiciv_hosp.diagnoses_icd diag
    ON icu.hadm_id = diag.hadm_id
  WHERE 
    ( -- ICD-9神经系统主诊断
      (diag.icd_version = 9 AND (
        diag.icd_code LIKE '430%' OR  -- 蛛网膜下腔出血
        diag.icd_code LIKE '431%' OR  -- 脑出血
        diag.icd_code LIKE '432%' OR  -- 颅内出血
        diag.icd_code LIKE '433%' OR  -- 脑动脉狭窄/堵塞
        diag.icd_code LIKE '434%' OR  -- 脑梗死
        diag.icd_code LIKE '435%' OR  -- 一过性脑缺血发作
        diag.icd_code LIKE '436%' OR
        diag.icd_code LIKE '437%' OR
        diag.icd_code LIKE '438%' OR
        diag.icd_code LIKE '852%' OR  -- 脑损伤
        diag.icd_code LIKE '853%' OR
        diag.icd_code LIKE '850%'     -- 脑震荡
      )) OR
      -- ICD-10神经系统主诊断
      (diag.icd_version = 10 AND (
        diag.icd_code LIKE 'I60%' OR
        diag.icd_code LIKE 'I61%' OR
        diag.icd_code LIKE 'I62%' OR
        diag.icd_code LIKE 'I63%' OR
        diag.icd_code LIKE 'I64%' OR
        diag.icd_code LIKE 'I65%' OR
        diag.icd_code LIKE 'I66%' OR
        diag.icd_code LIKE 'I67%' OR
        diag.icd_code LIKE 'I68%' OR
        diag.icd_code LIKE 'I69%' OR
        diag.icd_code LIKE 'G45%' OR
        diag.icd_code LIKE 'G46%'
      ))
    )
  GROUP BY icu.hadm_id, icu.stay_id
),

admit_type AS (
  SELECT hadm_id, admission_type
  FROM mimiciv_hosp.admissions
)

SELECT 
  hfzj.subject_id,
  hfzj.hadm_id,
  hfzj.stay_id,
  CASE 
    WHEN trauma_flag.is_trauma = 1 THEN 'trauma'
    WHEN neuro_diag_flag.is_neuro = 1 THEN 'neurology/neurosurgery'
    WHEN surgery_flag.has_surgery = 1 AND admit_type.admission_type = 'ELECTIVE' THEN 'elective surgery'
    WHEN surgery_flag.has_surgery = 1 AND admit_type.admission_type IN ('EMERGENCY', 'URGENT') THEN 'emergency surgery'
    ELSE 'medical'
  END AS admission_category
FROM hfzj
LEFT JOIN icu_time ON hfzj.stay_id = icu_time.stay_id
LEFT JOIN surgery_flag ON hfzj.hadm_id = surgery_flag.hadm_id AND hfzj.stay_id = surgery_flag.stay_id
LEFT JOIN trauma_flag ON hfzj.hadm_id = trauma_flag.hadm_id AND hfzj.stay_id = trauma_flag.stay_id
LEFT JOIN neuro_diag_flag ON hfzj.hadm_id = neuro_diag_flag.hadm_id AND hfzj.stay_id = neuro_diag_flag.stay_id
LEFT JOIN admit_type ON hfzj.hadm_id = admit_type.hadm_id
;







DROP TABLE IF EXISTS hfzj2;  
CREATE TEMP TABLE hfzj2 AS	
WITH
icu_time AS (
  SELECT stay_id, hadm_id, subject_id, intime
  FROM mimiciv_icu.icustays
),

bg_window AS (
  SELECT
    bg.subject_id,
    bg.hadm_id,
    bg.charttime,
    bg.pao2fio2ratio,
    icu.stay_id,
    icu.intime,
    ABS(EXTRACT(EPOCH FROM (bg.charttime - icu.intime))) AS diff_sec
  FROM mimiciv_derived.bg bg
  JOIN icu_time icu
    ON bg.hadm_id = icu.hadm_id AND bg.subject_id = icu.subject_id
  WHERE bg.pao2fio2ratio IS NOT NULL
    AND bg.charttime BETWEEN (icu.intime - INTERVAL '24 hours') AND (icu.intime + INTERVAL '24 hours')
),

nearest_bg AS (
  SELECT DISTINCT ON (stay_id)
    stay_id,
    pao2fio2ratio,
    charttime
  FROM bg_window
  ORDER BY stay_id, diff_sec ASC
)

SELECT
  hfzj.*,
  nearest_bg.pao2fio2ratio,
  nearest_bg.charttime AS bg_charttime,
  CASE
    WHEN nearest_bg.pao2fio2ratio < 300 THEN 1
    ELSE 0
  END AS resp_failure_oi_lt_300
FROM hfzj
LEFT JOIN nearest_bg ON hfzj.stay_id = nearest_bg.stay_id
;




DROP TABLE IF EXISTS hfzjbun;  
CREATE TEMP TABLE hfzjbun AS	
WITH
icu_time AS (
  SELECT stay_id, hadm_id, subject_id, intime
  FROM mimiciv_icu.icustays
),

bun_window AS (
  SELECT
    le.subject_id,
    le.hadm_id,
    le.charttime,
    le.valuenum AS bun_24h,
    icu.stay_id,
    icu.intime,
    ABS(EXTRACT(EPOCH FROM (le.charttime - icu.intime))) AS diff_sec
  FROM mimiciv_hosp.labevents le
  JOIN icu_time icu
    ON le.hadm_id = icu.hadm_id AND le.subject_id = icu.subject_id
  WHERE le.itemid = 51006  -- BUN
    AND le.valuenum IS NOT NULL
    AND le.charttime BETWEEN (icu.intime - INTERVAL '24 hours') AND (icu.intime + INTERVAL '24 hours')
),

nearest_bun AS (
  SELECT DISTINCT ON (stay_id)
    stay_id,
    bun_24h,
    charttime
  FROM bun_window
  ORDER BY stay_id, diff_sec ASC
)

SELECT
  hfzj.*,
  nearest_bun.bun_24h
FROM hfzj
LEFT JOIN nearest_bun ON hfzj.stay_id = nearest_bun.stay_id
;




DROP TABLE IF EXISTS hfzjmap;  
CREATE TEMP TABLE hfzjmap AS	
WITH
icu_time AS (
  SELECT stay_id, hadm_id, subject_id, intime
  FROM mimiciv_icu.icustays
),

map_window AS (
  SELECT
    ce.subject_id,
    ce.hadm_id,
    ce.charttime,
    ce.valuenum AS map_6h,
    icu.stay_id,
    icu.intime,
    ABS(EXTRACT(EPOCH FROM (ce.charttime - icu.intime))) AS diff_sec
  FROM mimiciv_icu.chartevents ce
  JOIN icu_time icu
    ON ce.stay_id = icu.stay_id
  WHERE ce.itemid IN (456, 220045)  
    AND ce.valuenum IS NOT NULL
    AND ce.charttime BETWEEN (icu.intime - INTERVAL '6 hours') AND (icu.intime + INTERVAL '6 hours')
),

nearest_map AS (
  SELECT DISTINCT ON (stay_id)
    stay_id,
    map_6h,
    charttime
  FROM map_window
  ORDER BY stay_id, diff_sec ASC, charttime DESC
)

SELECT
  hfzj.*,
  nearest_map.map_6h,
  nearest_map.charttime AS map_charttime
FROM hfzj
LEFT JOIN nearest_map ON hfzj.stay_id = nearest_map.stay_id
;





DROP TABLE IF EXISTS hfzjrz;  
CREATE TEMP TABLE hfzjrz AS	
WITH
icu_time AS (
  SELECT stay_id, hadm_id, subject_id
  FROM mimiciv_icu.icustays
),

-- 标记住院期间是否有认知障碍相关诊断
cog_impair_flag AS (
  SELECT 
    icu.stay_id,
    1 AS cognitive_impairment
  FROM icu_time icu
  JOIN mimiciv_hosp.diagnoses_icd diag
    ON icu.hadm_id = diag.hadm_id
  WHERE
    -- ICD-9
    diag.icd_code LIKE '290%'   -- 痴呆
    OR diag.icd_code LIKE '2941%'  -- 老年性痴呆
    OR diag.icd_code LIKE '3310%'  -- 阿尔茨海默病
    -- ICD-10
    OR diag.icd_code LIKE 'F00%'  -- 阿尔茨海默型痴呆
    OR diag.icd_code LIKE 'F01%'  -- 血管性痴呆
    OR diag.icd_code LIKE 'F02%'  -- 其他类型痴呆
    OR diag.icd_code LIKE 'F03%'  -- 未特指的痴呆
    OR diag.icd_code LIKE 'F05%'  -- 谵妄
    OR diag.icd_code LIKE 'G30%'  -- 阿尔茨海默病
    OR diag.icd_code LIKE 'G3184' -- 轻度认知障碍
  GROUP BY icu.stay_id
)

SELECT
  hfzj.*,
  COALESCE(cog_impair_flag.cognitive_impairment, 0) AS cognitive_impairment
FROM hfzj
LEFT JOIN cog_impair_flag ON hfzj.stay_id = cog_impair_flag.stay_id
;






DROP TABLE IF EXISTS hfzjjs;  
CREATE TEMP TABLE hfzjjs AS	
WITH
icu_time AS (
  SELECT stay_id, hadm_id, subject_id, intime
  FROM mimiciv_icu.icustays
),

steroid_flag AS (
  SELECT
    icu.stay_id,
    1 AS steroids
  FROM icu_time icu
  JOIN mimiciv_hosp.prescriptions drugs
    ON icu.hadm_id = drugs.hadm_id
  WHERE 
    drugs.starttime <= icu.intime  
    AND (
      drugs.drug ILIKE '%Methylprednisolone%' 
      OR drugs.drug ILIKE '%Prednisone%'
      OR drugs.drug ILIKE '%Hydrocortisone%'
      OR drugs.drug ILIKE '%Dexamethasone%'
      OR drugs.drug ILIKE '%Solumedrol%'
      OR drugs.drug ILIKE '%Cortisone%'
      OR drugs.drug ILIKE '%Betamethasone%'
    )
  GROUP BY icu.stay_id
)

SELECT
  hfzj.*,
  COALESCE(steroid_flag.steroids, 0) AS steroidsepre
FROM hfzj
LEFT JOIN steroid_flag ON hfzj.stay_id = steroid_flag.stay_id
;


CREATE TEMP TABLE asdx AS
SELECT 
    hfzj.*,
    hfzj1.admission_category,
    hfzj2.pao2fio2ratio,
    hfzjbun.bun_24h,
    hfzjmap.map_6h,
    hfzjrz.cognitive_impairment,
    hfzjjs.steroidsepre
FROM hfzj
LEFT JOIN hfzj1
    ON hfzj.subject_id = hfzj1.subject_id
LEFT JOIN hfzj2
    ON hfzj.subject_id = hfzj2.subject_id
LEFT JOIN hfzjbun
    ON hfzj.subject_id = hfzjbun.subject_id
LEFT JOIN hfzjmap
    ON hfzj.subject_id = hfzjmap.subject_id
LEFT JOIN hfzjrz
    ON hfzj.subject_id = hfzjrz.subject_id
LEFT JOIN hfzjjs
    ON hfzj.subject_id = hfzjjs.subject_id
		
	
	