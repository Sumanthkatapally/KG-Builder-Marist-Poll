# Neo4j Knowledge Graph Connection Script
Write-Host "Connecting to Neo4j Knowledge Graph: neo4j-kg-us-national-03052025-70de63d7" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Neo4j Browser: http://localhost:7475" -ForegroundColor Cyan
Write-Host "Bolt Connection: bolt://localhost:7688" -ForegroundColor Cyan
Write-Host "Username: neo4j" -ForegroundColor Cyan
Write-Host "Password: kg_us_national_03052025" -ForegroundColor Yellow
Write-Host ""

# Copy password to clipboard
Set-Clipboard -Value "kg_us_national_03052025"
Write-Host "Password copied to clipboard!" -ForegroundColor Green

# Open Neo4j Browser
Start-Process "http://localhost:7475"

Write-Host ""
Write-Host "Press any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
