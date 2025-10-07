#define MyAppName "LCDPR Frutacc"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Frutacc Comercio e Distriuição LTDA"
#define MyAppExeName "LCDPR Frutacc.exe"

#define MySourceRoot "C:\Users\conta\OneDrive\Documentos\LCDPR Frutacc"
#define MySourceMainExe "C:\Users\conta\OneDrive\Documentos\LCDPR Frutacc\LCDPR Frutacc.exe"
#define MySourceInternal "C:\Users\conta\OneDrive\Documentos\LCDPR Frutacc\_internal"

#define MyOutputDir "C:\Users\conta\OneDrive\Documentos\LCDPR Frutacc\installer"
#define MySetupIcon "C:\Users\conta\OneDrive\Documentos\LCDPR Frutacc\_internal\banco_de_dados\icons\agro_icon.ico"

[Setup]
AppId={{D3C2C1F7-9B0E-4B5D-AB2D-6C6A6B4A9C31}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={pf}\{#MyAppName}
DefaultGroupName={#MyAppName}
OutputDir={#MyOutputDir}
OutputBaseFilename=Instalador-{#MyAppName}-{#MyAppVersion}
Compression=lzma2
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin
WizardStyle=modern
SetupIconFile={#MySetupIcon}
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "portuguese"; MessagesFile: "compiler:Languages\BrazilianPortuguese.isl"

[Tasks]
Name: "desktopicon"; Description: "Criar atalho na área de trabalho"; GroupDescription: "Atalhos:"; Flags: unchecked

[Files]
; EXE principal
Source: "{#MySourceMainExe}"; DestDir: "{app}"; Flags: ignoreversion
; .env ao lado do EXE (será incluído se existir)
Source: "{#MySourceRoot}\.env"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
; Pasta _internal completa
Source: "{#MySourceInternal}\*"; DestDir: "{app}\_internal"; Flags: recursesubdirs createallsubdirs ignoreversion

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{commondesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Executar {#MyAppName} agora"; Flags: nowait postinstall skipifsilent
