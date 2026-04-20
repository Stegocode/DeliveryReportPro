; ──────────────────────────────────────────────────────────────────
; DeliveryReportPro Installer
; Built with Inno Setup 6.x  (https://jrsoftware.org/isinfo.php)
;
; HOW TO BUILD:
;   1. Install Inno Setup 6 on your dev machine
;   2. Run PyInstaller first:
;        pyinstaller DeliveryReportPro.spec --clean
;   3. Open this file in Inno Setup Compiler and click Build > Compile
;      OR from command line:
;        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" inno_setup.iss
;   4. Installer lands at: installer_output\DeliveryReportPro_Setup.exe
;
; INSTALL BEHAVIOR:
;   - Installs to %LOCALAPPDATA%\DeliveryReportPro\ — NO admin rights required
;   - Creates Desktop shortcut and Start Menu entry (current user only)
;   - Registers uninstaller under HKCU (no UAC, no IT friction)
;   - Config lives in %APPDATA%\DeliveryReportPro\ (separate from app files)
; ──────────────────────────────────────────────────────────────────

#define AppName      "DeliveryReportPro"
#define AppVersion   "1.0.0"
#define AppPublisher "Your Company Name"
#define AppExeName   "DeliveryReportPro.exe"
#define AppURL       "https://example.com"

[Setup]
AppId={{9E6384C8-83FD-458B-8ACF-E2B9776CD78A}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}

; Install to LOCALAPPDATA — no admin rights required
DefaultDirName={localappdata}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes

; Output — paths are relative to this .iss file's location
OutputDir=installer_output
OutputBaseFilename=DeliveryReportPro_Setup
SourceDir=.

; Compression
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; UI
WizardStyle=modern
WizardSizePercent=120
DisableWelcomePage=no
DisableDirPage=no
DisableReadyPage=no

; No admin rights needed
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=

; Version info on the installer exe
VersionInfoVersion={#AppVersion}
VersionInfoCompany={#AppPublisher}
VersionInfoDescription={#AppName} Setup
VersionInfoProductName={#AppName}
VersionInfoProductVersion={#AppVersion}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &Desktop shortcut"; \
      GroupDescription: "Additional icons:"

[Files]
; Main application — everything PyInstaller produced
Source: "dist\DeliveryReportPro\*"; DestDir: "{app}"; \
        Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; Start Menu
Name: "{group}\{#AppName}";          Filename: "{app}\{#AppExeName}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"

; Desktop shortcut (optional, checked by default above)
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; \
      Tasks: desktopicon

[Run]
; Launch after install — unchecked by default so user can choose
Filename: "{app}\{#AppExeName}"; \
          Description: "Launch {#AppName} now"; \
          Flags: nowait postinstall skipifsilent unchecked

[UninstallDelete]
; Clean up ChromeDriver and scrape inbox on uninstall
; Config in APPDATA is intentionally left (preserves credentials)
Type: filesandordirs; Name: "{localappdata}\{#AppName}"

[Code]
// ── Pre-install: check Chrome is present ────────────────────────
function ChromeInstalled(): Boolean;
var
  version: string;
begin
  Result := RegQueryStringValue(HKCU,
    'Software\Google\Chrome\BLBeacon',
    'version', version) or
  RegQueryStringValue(HKLM,
    'Software\Google\Chrome\BLBeacon',
    'version', version) or
  RegQueryStringValue(HKLM,
    'Software\Wow6432Node\Google\Chrome\BLBeacon',
    'version', version);
end;

function InitializeSetup(): Boolean;
var
  msg: string;
begin
  Result := True;
  if not ChromeInstalled() then begin
    msg := 'Google Chrome does not appear to be installed on this machine.'
      + Chr(13) + Chr(10) + Chr(13) + Chr(10)
      + 'DeliveryReportPro uses Chrome to securely access HomeSource. '
      + 'Please install Chrome from google.com/chrome before using DeliveryReportPro.'
      + Chr(13) + Chr(10) + Chr(13) + Chr(10)
      + 'Continue installing DeliveryReportPro anyway?';
    if MsgBox(msg, mbConfirmation, MB_YESNO) = IDNO then
      Result := False;
  end;
end;

// ── Post-install: create APPDATA config dir ──────────────────────
procedure CurStepChanged(CurStep: TSetupStep);
var
  configDir: string;
begin
  if CurStep = ssPostInstall then begin
    configDir := ExpandConstant('{userappdata}\DeliveryReportPro');
    if not DirExists(configDir) then
      CreateDir(configDir);
  end;
end;
