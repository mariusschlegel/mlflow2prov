import prov.constants


class ChangeType:
    ADDED = "A"
    MODIFIED = "M"
    DELETED = "D"


class ProvRole:
    COMMIT = "Commit"
    COMMITTER = "Committer"
    AUTHOR = "Author"
    COMMIT_AUTHOR = "CommitAuthor"
    TAG_AUTHOR = "TagAuthor"
    FILE = "File"
    ADDED_REVISION = "AddedRevision"
    DELETED_REVISION = "DeletedRevision"
    MODIFIED_REVISION = "ModifiedRevision"
    PREVIOUS_REVISION = "PreviousRevision"
    ADDED_EXPERIMENT = "AddedExperiment"
    ADDED_EXPERIMENT_TAG = "AddedExperimentTag"
    EXPERIMENT_AUTHOR = "ExperimentAuthor"
    ADDED_RUN = "AddedRun"
    ADDED_RUN_TAG = "AddedRunTag"
    RUN_AUTHOR = "RunAuthor"
    ADDED_REGISTERED_MODEL = "AddedRegisteredModel"
    REGISTERED_MODEL_AUTHOR = "RegisteredModelVersionAuthor"
    ADDED_REGISTERED_MODEL_VERSION = "AddedRegisteredModelVersion"
    REGISTERED_MODEL_VERSION_AUTHOR = "RegisteredModelVersionAuthor"
    DELETED_EXPERIMENT = "DeletedExperiment"
    DELETED_RUN = "DeletedRun"
    DELETED_REGISTERED_MODEL_VERSION = "DeletedRegisteredModelVersion"


class ProvType:
    USER = "User"
    COMMIT = "Commit"
    FILE = "File"
    FILE_REVISION = "FileRevision"
    COMMIT = "Commit"
    CREATION = "Creation"
    DELETION = "Deletion"
    EXPERIMENT = "Experiment"
    EXPERIMENT_TAG = "ExperimentTag"
    RUN = "Run"
    RUN_TAG = "RunTag"
    METRIC = "Metric"
    PARAM = "Param"
    ARTIFACT = "Artifact"
    MODEL_ARTIFACT = "ModelArtifact"
    REGISTERED_MODEL = "RegisteredModel"
    REGISTERED_MODEL_TAG = "RegisteredModelTag"
    REGISTERED_MODEL_VERSION = "RegisteredModelVersion"
    REGISTERED_MODEL_VERSION_TAG = "RegisteredModelVersionTag"
    COLLECTION = prov.constants.PROV_ATTR_COLLECTION
