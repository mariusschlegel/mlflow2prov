CALL {
    MATCH
    (run:Entity)-[:hadMember]-(metric:Entity),
    (run:Entity)-[:hadMember]-(runTagUser:Entity),
    (run:Entity)-[:hadMember]-(runTagSourceName:Entity),
    (run:Entity)-[:wasGeneratedBy]-(runCreation:Activity)

    WHERE
    metric.`prov:type`="Metric" AND
    metric.name="r2" AND
    runTagUser.`prov:type`="RunTag" AND
    runTagUser.name="mlflow.user" AND
    runTagSourceName.`prov:type`="RunTag" AND
    runTagSourceName.name="mlflow.source.name"

    RETURN
    run.run_id AS model_run_id,
    metric.value AS r2,
    runTagUser.value AS created_by,
    runCreation.`prov:startTime` AS created_at,
    runTagSourceName.value AS run_source

    ORDER BY
    metric.value DESC LIMIT 1
}

MATCH
(commit:Activity)-[:wasGeneratedBy]-(fileRevision:Entity),
(user:Agent)-[:wasAssociatedWith]-(commit:Activity)

WHERE
commit.`prov:type`="Commit" AND
fileRevision.name=run_source AND
fileRevision.`prov:type` = "FileRevision"

WITH
model_run_id,
r2,
created_by,
created_at,
run_source,
user.name AS contributed_most,
COUNT(DISTINCT commit) AS commit_count

RETURN
model_run_id,
r2,
created_by,
created_at,
contributed_most

ORDER BY commit_count DESC

LIMIT 1
