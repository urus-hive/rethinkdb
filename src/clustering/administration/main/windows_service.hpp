// Copyright 2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_MAIN_WINDOWS_SERVICE_HPP_
#define CLUSTERING_ADMINISTRATION_MAIN_WINDOWS_SERVICE_HPP_
#ifdef _WIN32

#include <exception>
#include <string>
#include <vector>

class windows_privilege_exc_t : public std::exception {
};

// Helper function to escape a path or argument
// For example
//   c:\test folder\test.exe
// gets turned into
//   "c:\\test folder\\test.exe"
std::string escape_windows_shell_arg(const std::string &arg);

void install_windows_service(
	const std::string &service_name,
	const std::string &bin_path,
	const std::vector<std::string> &arguments);

void remove_windows_service(const std::string &service_name);

// TODO! Start and stop the service

#endif /* _WIN32 */
#endif /* CLUSTERING_ADMINISTRATION_MAIN_WINDOWS_SERVICE_HPP_ */