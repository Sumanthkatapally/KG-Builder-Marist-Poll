# Neo4j Knowledge Graph Connection Script
Write-Host "Connecting to Neo4j Knowledge Graph: neo4j-kg-us-national-04032024-0905aa37" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Neo4j Browser: http://localhost:7474" -ForegroundColor Cyan
Write-Host "Bolt Connection: bolt://localhost:7687" -ForegroundColor Cyan
Write-Host "Username: neo4j" -ForegroundColor Cyan
Write-Host "Password: kg_us_national_04032024" -ForegroundColor Yellow
Write-Host ""

# Copy password to clipboard
Set-Clipboard -Value "kg_us_national_04032024"
Write-Host "Password copied to clipboard!" -ForegroundColor Green

# Open Neo4j Browser
Start-Process "http://localhost:7474"

Write-Host ""
Write-Host "Press any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
