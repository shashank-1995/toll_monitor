#define MyAppName "Toll Audit"
#define MyAppVersion "1.0.0"
#define MyAppExeName "toll_audit.exe"

[Setup]
AppId={{A1B2C3D4-E5F6-47A1-ABCD-1234567890AB}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
DefaultDirName={autopf}\TollAudit
DisableProgramGroupPage=yes
OutputDir=output
OutputBaseFilename=TollAuditInstaller
Compression=lzma
SolidCompression=yes

[Files]
; We’ll copy the onedir build output folder
Source: "..\dist\toll_audit\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
Name: "{app}\Files"
Name: "{app}\Files2"
Name: "{app}\logs"

[Icons]
Name: "{commondesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Run {#MyAppName}"; Flags: nowait postinstall skipifsilent

