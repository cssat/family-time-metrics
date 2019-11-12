WITH "childDetailsRaw" AS (
SELECT
    id,
    json_array_elements("childDetails") ->> 'childFamlinkPersonID' id_prsn_child,
    json_array_elements("childDetails") ->> 'childOpd' original_placement_date_dirty
FROM staging. "ServiceReferrals"
WHERE
    "requestDate" BETWEEN '2019-10-01' AND now()
    AND "formVersion" = 'Ingested'
    AND "organizationId" != 1
), "parentDetailsRaw" AS (
SELECT
    id,
    json_array_elements("parentGuardianDetails") ->> 'parentGuardianId' id_prsn_parent
FROM staging. "ServiceReferrals"
WHERE
    "requestDate" BETWEEN '2019-10-01'
    AND now()
    AND "formVersion" = 'Ingested'
    AND "organizationId" != 1
), "childDetails" AS (
SELECT DISTINCT
    id,
    CASE WHEN id_prsn_child IS NOT NULL THEN
        id_prsn_child
    ELSE
        0::TEXT
    END id_prsn_child,
    CASE WHEN original_placement_date_dirty ~* '-' THEN
        TO_DATE(original_placement_date_dirty, 'YYYY-MM-DD')
    WHEN original_placement_date_dirty ~* '/' THEN
        TO_DATE(original_placement_date_dirty, 'MM/DD/YYYY')
    END AS original_placement_date
FROM "childDetailsRaw"
), "parentDetails" AS (
SELECT DISTINCT
    id,
    CASE WHEN id_prsn_parent::INTEGER IS NOT NULL THEN
        id_prsn_parent
    ELSE
        0::TEXT
    END id_prsn_parent
FROM "parentDetailsRaw"
), "childAndParentDetails" AS (
SELECT
    sr.id,
    sr."caseNumber" id_case,
    COALESCE(id_prsn_child, 0::TEXT) id_prsn_child,
    COALESCE(id_prsn_parent, 0::TEXT) id_prsn_parent,
    original_placement_date,
    DENSE_RANK() OVER (ORDER BY
        COALESCE(id_prsn_child, 0::TEXT),
        sr."caseNumber",
        COALESCE(id_prsn_parent, 0::TEXT)) visitation_group
FROM staging. "ServiceReferrals" sr
  LEFT JOIN "childDetails" cd
    ON sr.id = cd.id
  LEFT JOIN "parentDetails" pd
    ON pd.id = cd.id
)

SELECT DISTINCT CASE
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
	,NULL::INTEGER AS "organizationId"
	,"requestDate" AS "date"
	,'visitation_needed' AS "from"
	,'sw_request' AS "to"
FROM staging."ServiceReferrals" AS sr
  LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1

UNION

SELECT DISTINCT CASE
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
	,NULL::INTEGER AS "organizationId"
	,"visitPlanApprovedDate" AS "date"
	,'sw_request' AS "from"
	,'approved' AS "to"
FROM staging."ServiceReferrals" AS sr
  LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1

UNION

SELECT CASE
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
	,"organizationId"
	,min(sr."updatedAt") AS "date"
	,'approved' AS "from"
	,'vc_received' AS "to"
FROM staging."ServiceReferrals" AS sr
  LEFT JOIN staging."Organizations" AS o
    ON sr."organizationId" = o.id
  LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1
	AND sr."organizationId" IN (
		SELECT id
		FROM staging."Organizations"
		WHERE "routingOrg"
		)
GROUP BY "initialVisitPlanId"
	,"visitPlanId"
	,"organizationId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group

UNION

SELECT DISTINCT CASE
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
	,"OrganizationId"
	,DATE
	,CASE
		WHEN "StageTypeId" = 7
			THEN 'vc_received'
		WHEN "StageTypeId" IN (
				8
				,12
				)
			THEN 'vc_received_timeline'
		WHEN "StageTypeId" = 9
			THEN 'accepted'
		WHEN "StageTypeId" = 10
			THEN 'visit_schedule_confirmed'
		WHEN "StageTypeId" = 11
			THEN 'not_sure'
		ELSE ''
		END AS "from"
	,CASE
		WHEN "StageTypeId" = 7
			THEN 'vc_received_timeline'
		WHEN "StageTypeId" = 8
			THEN 'accepted'
		WHEN "StageTypeId" = 9
			THEN 'visit_schedule_confirmed'
		WHEN "StageTypeId" = 10
			THEN 'first_visit_expected'
		WHEN "StageTypeId" = 11
			THEN 'resolved'
		WHEN "StageTypeId" = 12
			THEN 'rejected'
		ELSE ''
		END AS "to"
FROM staging."ServiceReferrals" AS sr
  LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
LEFT JOIN (
	SELECT "ServiceReferralId"
		,"StageTypeId"
		,"OrganizationId"
		,min(DATE) AS DATE
	FROM staging."ServiceReferralTimelineStages"
	GROUP BY "ServiceReferralId"
		,"StageTypeId"
		,"OrganizationId"
	) AS srts ON sr.id = srts."ServiceReferralId"
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1

UNION

SELECT "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
  ,"organizationId"
  ,"updatedAt" AS DATE
  ,'vc_received_timeline' AS 'from'
  ,'vc_received_timeline' AS 'to'
FROM (
	SELECT "initialVisitPlanId"
		,"visitPlanId"
    ,id_case
    ,id_prsn_child
    ,id_prsn_parent
    ,original_placement_date
    ,visitation_group
		,"organizationId"
		,"updatedAt"
		,"routingOrg"
		,LEAD("organizationId", 1, NULL) OVER (
			PARTITION BY "initialVisitPlanId" ORDER BY "versionId"
			) AS lead_org
		,CASE
			WHEN "routingOrg" IS NOT NULL
				AND LEAD("routingOrg", 1, NULL) OVER (
					PARTITION BY "initialVisitPlanId" ORDER BY "versionId"
					) IS NOT NULL
				THEN CASE
						WHEN "organizationId" != LEAD("organizationId", 1, NULL) OVER (
								PARTITION BY "initialVisitPlanId" ORDER BY "versionId"
								)
							THEN 1
						END
			END new_org
	FROM (
		SELECT "versionId"
			,CASE
				WHEN "initialVisitPlanId" = 0
					THEN "visitPlanId"::INTEGER
				ELSE "initialVisitPlanId"
				END AS "initialVisitPlanId"
			,"visitPlanId"
      ,id_case
      ,id_prsn_child
      ,id_prsn_parent
      ,original_placement_date
      ,visitation_group
			,"organizationId"
			,sr."updatedAt"
			,o."routingOrg"
		FROM staging."ServiceReferrals" AS sr
		LEFT JOIN staging."Organizations" AS o ON o.id = sr."organizationId"
			AND o."routingOrg"
		LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
		WHERE "requestDate" BETWEEN '2019-10-01'
				AND now()
			AND "formVersion" = 'Ingested'
			AND "organizationId" != 1
		) AS new_ro
	) AS dat
WHERE new_org = 1

UNION

SELECT CASE
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
  ,id_case
  ,id_prsn_child
  ,id_prsn_parent
  ,original_placement_date
  ,visitation_group
	,"organizationId"
	,"deletedAt" AS "date"
	,'vc_received_timeline' AS "from"
	,'deleted' AS "to"
FROM staging."ServiceReferrals" sr
  LEFT JOIN "childAndParentDetails" cd
    ON sr.id = cd.id
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1
	AND "deletedAt" IS NOT NULL
