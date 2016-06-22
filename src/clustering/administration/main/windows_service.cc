// Copyright 2016 RethinkDB, all rights reserved.
#include "clustering/administration/main/windows_service.hpp"

#ifdef _WIN32

#include <windows.h>

std::string escape_windows_shell_arg(const std::string &arg) {
	std::string res;
	res += "\"";
	for (char ch : arg) {
		if (ch == '\\') {
			res += "\\\\";
		} else if (ch == '"') {
			res += "\\\"";
		} else {
			res += ch;
		}
	}
	res += "\"";
	return res;
}

class service_manager_t {
public:
	explicit service_manager_t(DWORD desired_access) {
		sc_manager = OpenSCManager(
			nullptr /* local computer */,
			nullptr,
			desired_access);
		if (sc_manager == nullptr) {
			DWORD err = GetLastError();
			if (err == ERROR_ACCESS_DENIED) {
				throw windows_privilege_exc_t();
			}
			// TODO!
			fprintf(stderr, "Error opening service manager\n");
		}
	}
	~service_manager_t() {
		if (sc_manager != nullptr) {
			CloseServiceHandle(sc_manager);
		}
	}
	SC_HANDLE get() {
		return sc_manager;
	}
private:
	SC_HANDLE sc_manager;
};

void install_windows_service(
	const std::string &service_name,
	const std::string &bin_path,
	const std::vector<std::string> &arguments) {

	// The following code is loosely based on the example at
	// https://msdn.microsoft.com/en-us/library/windows/desktop/ms683500(v=vs.85).aspx

	// Get a handle to the SCM database. 
	service_manager_t sc_manager(SC_MANAGER_CREATE_SERVICE);

	// Build the service path (escaped binary path + arguments)
	std::string service_path = escape_windows_shell_arg(bin_path);
	for (const auto &arg : arguments) {
		service_path += " ";
		service_path += escape_windows_shell_arg(arg);
	}

	// TODO! Are there restrictions on service_name?
	// Create the service
	SC_HANDLE service = CreateService(
		sc_manager.get(),
		service_name.c_str(), /* name of service */
		service_name.c_str(), /* service name to display */
		SERVICE_ALL_ACCESS,
		SERVICE_WIN32_OWN_PROCESS,
		SERVICE_AUTO_START /* start the service on system startup */,
		SERVICE_ERROR_NORMAL,
		service_path.c_str(),
		nullptr /* no load ordering group  */,
		nullptr /* no tag identifier */,
		nullptr /* no dependencies */,
		nullptr /* LocalSystem account */, // TODO!
		nullptr /* no password */);

	if (service == nullptr) {
		DWORD err = GetLastError();
		if (err == ERROR_ACCESS_DENIED) {
			throw windows_privilege_exc_t();
		}
		// TODO!
		fprintf(stderr, "Creating the service failed\n");
		return;
	} else {
		CloseServiceHandle(service);
	}
}

void remove_windows_service(const std::string &service_name) {

	// Get a handle to the SCM database. 
	service_manager_t sc_manager(SC_MANAGER_CONNECT);

	// TODO! Are there restrictions on service_name?

	// Open the service
	SC_HANDLE service = OpenService(
		sc_manager.get(),
		service_name.c_str(),
		DELETE);

	if (service == nullptr) {
		DWORD err = GetLastError();
		if (err == ERROR_ACCESS_DENIED) {
			throw windows_privilege_exc_t();
		}
		// TODO!
		fprintf(stderr, "Opening the service failed\n");
		return;
	}

	// Delete the service
	if (!DeleteService(service)) {
		CloseServiceHandle(service);
		DWORD err = GetLastError();
		if (err == ERROR_ACCESS_DENIED) {
			throw windows_privilege_exc_t();
		}
		// TODO!
		fprintf(stderr, "Deleting the service failed\n");
		return;
	} else {
		// TODO!
		fprintf(stderr, "Deleted service\n");
		CloseServiceHandle(service);
	}
}

#endif /* _WIN32 */
