#DATA_ROOT="${DATA_ROOT="CTBrain Datasets"}"
#TARGET_DS_NAME="${DS_NAME="test"}"
#ADDED_DS_PATH="${ADDED_DS_NAME="~/datasets/Artifact"}"

dvc add "${DATA_ROOT}/${TARGET_DS_NAME}"
dvc push
git add "${DATA_ROOT}/*.dvc" "${DATA_ROOT}/.gitignore" .gitignore .dvcignore .dvc/config .dvc/.gitignore
git commit -m "add ${TARGET_DS_NAME} datasource in ${DATA_ROOT}"
#git push origin main