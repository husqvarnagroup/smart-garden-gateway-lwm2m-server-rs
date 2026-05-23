Code
* Run "cargo fmt" before each commit
* Ensure that "cargo clippy" and "cargo test" passes before each commit

Logging
* Log message rules
  * Start with capital letter
  * INFO, WARN and ERROR are read by a troubleshoort. Avoid too technical terms that require knowledge of the code.
  * DEBUG are only read by developers, can be technical / reference concepts from code (variable names, etc.)
* Include following structured fields if available / applicable
  * device: the sgtin of the device
  * activity: inclusion, exclusion, state, registration, fota, connection-status, or control
