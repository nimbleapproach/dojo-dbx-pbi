
## Windows Installation

### 1. Downloading the Executable

- Clone this repository using Git or GitHub Desktop installed on your AVD.
- Navigate to the repository and access the directory `executables_amd64/windows`.

### 2. Save the Executable

- Copy `databricks.exe` from the folder with the highest version to your desired directory. For this guide, let's assume you've saved it in `C:\Users\YourUsername\databricks`.

### 3. Add the Directory to the PATH

To add the directory to the PATH for your user account, follow these steps:

a. Press `Win + R` to open the Run dialog.  
b. Type `rundll32.exe sysdm.cpl,EditEnvironmentVariables` and hit Enter to open the Environment Variables window for your account.  
c. In the "User variables for [YourUsername]" section, find the `PATH` or `Path` variable and select it. If it doesn't exist, click `New`.  
d. Click `Edit`. 
   - For a new `PATH` variable, set the value to `C:\Users\YourUsername\databricks`.
   - For an existing `PATH` variable:
     i. Click `New` in the "Edit Environment Variable" window.  
     ii. Add `C:\Users\YourUsername\databricks`.  
e. Click `OK` to confirm and close all windows.

### 4. Restart Your PC

To apply the changes, restart your AVD.

### 5. Test the Installation

- Open a new Command Prompt (cmd) or PowerShell window after the restart.
- Type `databricks.exe` and hit Enter. If the program runs, the setup was successful.

## Databricks CLI Configuration

### 1. Generate a New Personal Access Token

- Navigate to the ANWO environment and [generate your personal access token](https://adb-4827527652115658.18.azuredatabricks.net/settings/user/developer/access-tokens/?o=4827527652115658).
- Copy the token for future use.

### 2. Configure Databricks

- Open a terminal and enter:  
  `databricks configure`
- Provide the following values:  
  Databricks Host: `https://adb-4827527652115658.18.azuredatabricks.net`  
  Personal Access Token: `PASTE YOUR TOKEN HERE`

### 3. Verify the Configuration File

- The Databricks CLI should create a `.databrickscfg` file in your home directory, i.e., `C:\Users\YourUsername`.
- This file should contain the previously entered values, with the first line indicating the [DEFAULT] profile.

### 4. Verify Authentication

- Open a terminal and enter:  
  `databricks jobs list`
- If you see jobs from the ANWO environment, the setup is correct.
