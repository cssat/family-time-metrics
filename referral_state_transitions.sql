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
	,'requested' AS "to"
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
