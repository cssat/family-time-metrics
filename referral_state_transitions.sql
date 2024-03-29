SELECT DISTINCT CASE 
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
	,NULL::INTEGER AS "organizationId"
	,"requestDate" AS "date"
	,'visitation_needed' AS "from"
	,'sw_request' AS "to"
FROM staging."ServiceReferrals" AS sr
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
	,NULL::INTEGER AS "organizationId"
	,"visitPlanApprovedDate" AS "date"
	,'sw_request' AS "from"
	,'approved' AS "to"
FROM staging."ServiceReferrals" AS sr
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
	,"organizationId"
	,min(sr."updatedAt") AS "date"
	,'approved' AS "from"
	,'vc_received' AS "to"
FROM staging."ServiceReferrals" AS sr
LEFT JOIN staging."Organizations" AS o ON sr."organizationId" = o.id
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

UNION

SELECT DISTINCT CASE 
		WHEN "initialVisitPlanId" = 0
			THEN "visitPlanId"::INTEGER
		ELSE "initialVisitPlanId"
		END AS "initialVisitPlanId"
	,"visitPlanId"
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
	,"organizationId"
	,"updatedAt" AS DATE
	,'vc_received_timeline' AS
FROM
	,'vc_received' AS TO
FROM (
	SELECT "initialVisitPlanId"
		,"visitPlanId"
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
			,"organizationId"
			,sr."updatedAt"
			,o."routingOrg"
		FROM staging."ServiceReferrals" AS sr
		LEFT JOIN staging."Organizations" AS o ON o.id = sr."organizationId"
			AND o."routingOrg"
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
	,"organizationId"
	,"deletedAt" AS "date"
	,'vc_received_timeline' AS "from"
	,'deleted' AS "to"
FROM staging."ServiceReferrals"
WHERE "requestDate" BETWEEN '2019-10-01'
		AND now()
	AND "formVersion" = 'Ingested'
	AND "organizationId" != 1
	AND "deletedAt" IS NOT NULL
