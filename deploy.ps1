# ===============================
# CONFIGURACIÓN INICIAL
# ===============================
gcloud config set project mintic-models-prod
gcloud config set compute/region us-central1

$IMAGE_NAME = "causaciones-recaudos-cosecha"
$JOB_NAME   = "causaciones-recaudos-cosecha-job" 
$REPO_PATH  = "us-central1-docker.pkg.dev/mintic-models-prod/causaciones-recaudos-cosecha/$IMAGE_NAME"
# ===============================
# VERIFICAR DOCKER
# ===============================
Write-Host "Verificando Docker..." -ForegroundColor Cyan
try {
    docker ps | Out-Null
    Write-Host "[OK] Docker corriendo" -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Docker no está corriendo" -ForegroundColor Red
    exit 1
}

# ===============================
# CONFIRMACIÓN DE DEPLOY
# ===============================
Write-Host "`n*** DEPLOYING JOB TO PRODUCTION ***" -ForegroundColor Red
$confirm = Read-Host "Escribe 'SI' para continuar"
if ($confirm -ne 'SI') {
    Write-Host "Deploy cancelado" -ForegroundColor Yellow
    exit
}

# ===============================
# BUILD IMAGEN
# ===============================
Write-Host "`nConstruyendo imagen..." -ForegroundColor Cyan
docker build -t $IMAGE_NAME .

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Error al construir imagen" -ForegroundColor Red
    exit 1
}

# ===============================
# TAG & PUSH
# ===============================
Write-Host "`nGenerando TAG..." -ForegroundColor Cyan
docker tag $IMAGE_NAME $REPO_PATH

Write-Host "`nSubiendo imagen a Artifact Registry..." -ForegroundColor Cyan
docker push $REPO_PATH

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Error al subir imagen" -ForegroundColor Red
    exit 1
}

# ===============================
# DEPLOY CLOUD RUN JOB
# ===============================
Write-Host "`nDesplegando Cloud Run Job..." -ForegroundColor Cyan

gcloud run jobs deploy $JOB_NAME `
  --image $REPO_PATH `
  --region us-central1 `
  --memory 8Gi `
  --cpu 2 `
  --task-timeout 3600 `
  --max-retries 3 `

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[OK] JOB DESPLEGADO EXITOSAMENTE" -ForegroundColor Green
    Write-Host "Nombre del job: $JOB_NAME" -ForegroundColor Cyan
    Write-Host "Región: us-central1" -ForegroundColor Cyan
    Write-Host "`nPara ejecutar manualmente:" -ForegroundColor Yellow
    Write-Host "gcloud run jobs execute $JOB_NAME --region us-central1" -ForegroundColor White
}
else {
    Write-Host "[ERROR] Error al desplegar el Job" -ForegroundColor Red
    exit 1
}
