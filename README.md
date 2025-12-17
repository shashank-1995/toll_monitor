Toll Audit Trial - Executable Package

1. Purpose
This trial executable demonstrates how your existing toll audit script can be
packaged into a desktop-ready application. The logic inside this executable
is the same as in your Python source files, which are linked together as:

  main_edited.py  -> imports rough7.py
  rough7.py      -> imports constants2.py and dbInfo.py

Because of this import chain, all of the original logic (constants, DB access,
and audit rules) is bundled inside toll_audit.exe.

2. Requirements
- Windows PC
- Wampserver/MySQL installed and running
- Your existing database and tables already set up:
    t_statement
    toll_d
    tag_class
- db connection parameters configured the same way as in your original script
  (for example in dbInfo.py or equivalent).

3. How to Run
1) Copy toll_audit.exe into the folder where you normally run your Python script
   (or wherever your DB configuration is expected).
2) Ensure Wampserver/MySQL is running and your database is accessible.
3) Run from Command Prompt:
       toll_audit.exe
   or double-click toll_audit.exe in Explorer.
4) The executable will:
   - Connect to your existing MySQL database.
   - Use the same logic as your Python code (through main_edited.py, rough7.py,
     constants2.py, dbInfo.py).
   - Generate audit/dispute outputs in the same way as your current script.

4. Notes
- No data is sent to any external server; all processing is local.
- This trial focuses on packaging and running your existing logic.
- In the extended version, constants will be loaded from a central link on
  your server (simple GET request) so that updating one file on your side
  updates all user PCs without reinstalling.

If you see any error, please capture:
- The full error message or screenshot
- A short description of what you ran and on which machine
so the packaging can be adjusted if needed.
