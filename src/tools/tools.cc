#include <sys/stat.h>
#include <sys/socket.h>
#include <linux/un.h>
#include <arpa/inet.h>
#include <string>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include "errno.h"
#include "stdio.h"
#include "common/errno.h"
#include "include/inttypes.h"
#include "common/safe_io.h"
#include "tools/tools.h"
using namespace std;

int ceph_tool_do_admin_socket(std::string path, std::string cmd, std::ostream& output)
{
  struct sockaddr_un address;
  int fd;
  int r;

  fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if(fd < 0) {
    cerr << "socket failed with " << cpp_strerror(errno) << std::endl;
    return -1;
  }

  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, UNIX_PATH_MAX, "%s", path.c_str());

  if (connect(fd, (struct sockaddr *) &address,
	      sizeof(struct sockaddr_un)) != 0) {
    cerr << "connect to " << path << " failed with " << cpp_strerror(errno) << std::endl;
    return -1;
  }

  char *buf;
  uint32_t len;
  r = safe_write(fd, cmd.c_str(), cmd.length() + 1);
  if (r < 0) {
    cerr << "write to " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }

  r = safe_read(fd, &len, sizeof(len));
  if (r < 0) {
    cerr << "read " << len << " length from " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }
  if (r < 4) {
    cerr << "read only got " << r << " bytes of 4 expected for response length; invalid command?" << std::endl;
    goto out;
  }
  len = ntohl(len);

  buf = new char[len+1];
  r = safe_read(fd, buf, len);
  if (r < 0) {
    cerr << "read " << len << " bytes from " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }
  buf[len] = '\0';

  output << buf << std::endl;
  r = 0;

 out:
  ::close(fd);
  return r;
}
