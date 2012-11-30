#ifndef CEPH_TOOLS_TOOLS_DOT_H
#define CEPH_TOOLS_TOOLS_DOT_H
#include <string>
#include <ostream>

int ceph_tool_do_admin_socket(std::string path, std::string cmd, std::ostream& output);

#endif
