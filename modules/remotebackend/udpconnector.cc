#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "remotebackend.hh"
#include <sys/socket.h>
#include "pdns/lock.hh" 
#include <unistd.h>
#include <fcntl.h>
#ifndef UNIX_PATH_MAX 
#define UNIX_PATH_MAX 108
#endif

UdpsocketConnector::UdpsocketConnector(std::map<std::string,std::string> options) {
  L<<Logger::Info<<"creating UdpsocketConnector::UdpsocketConnector"<<endl;
  if (options.count("address") == 0) {
    L<<Logger::Error<<"Cannot find 'address' option in connection string"<<endl;
    throw PDNSException();
  } 
  this->timeout = 2000;
  if (options.find("timeout") != options.end()) { 
    this->timeout = boost::lexical_cast<int>(options.find("timeout")->second);
  }
  this->address = options.find("address")->second;
  this->options = options;
  this->connected = false;
  this->fd = -1;
  L<<Logger::Info<<"found address: "<<address<<endl;
}

UdpsocketConnector::~UdpsocketConnector() {
  if (this->connected) {
     L<<Logger::Info<<"closing socket connection"<<endl;
     close(fd);
  }
}

int UdpsocketConnector::send_message(const rapidjson::Document &input) {
  std::string data;
  int rv;
  data = makeStringFromDocument(input);
  data = data + "\n";
  rv = this->write(data);
  if (rv == -1)
    return -1;
  return rv;
}

int UdpsocketConnector::recv_message(rapidjson::Document &output) {
  int rv;
  std::string s_output;
  rapidjson::GenericReader<rapidjson::UTF8<> , rapidjson::MemoryPoolAllocator<> > r;

  struct timeval t0,t;

  gettimeofday(&t0, NULL);
  memcpy(&t,&t0,sizeof(t0));
  s_output = "";       

  while((t.tv_sec - t0.tv_sec)*1000 + (t.tv_usec - t0.tv_usec)/1000 < this->timeout) { 
    int avail = waitForData(this->fd, 0, this->timeout * 500); // use half the timeout as poll timeout
    if (avail < 0)
      return -1;
    if (avail == 0) {
      //L<<Logger::Info<<"poll timeout"<<endl;
      gettimeofday(&t, NULL);
      continue;
    }

    rv = this->read(s_output);
    if (rv == -1) 
      return -1;

    if (rv>0) {
      //L<<Logger::Info<<"got response: "<<s_output<<endl;
      rapidjson::StringStream ss(s_output.c_str());
      output.ParseStream<0>(ss); 
      if (output.HasParseError() == false) {
        return s_output.size();
      }
    }
    gettimeofday(&t, NULL);
  }

  close(fd);
  connected = false; // we need to reconnect
  return -1;
}

ssize_t UdpsocketConnector::read(std::string &data) {
  ssize_t nread;
  //char buf[1500] = {0};
  char buf[8192] = {0}; // should be enough?

  reconnect();
  if (!connected) return -1;
  nread = ::read(this->fd, buf, sizeof buf);

  // just try again later...
  if (nread==-1 && errno == EAGAIN) return 0;

  if (nread==-1) {
    connected = false;
    close(fd);
    return -1;
  }

  data.append(buf, nread);
  return nread;
}

ssize_t UdpsocketConnector::write(const std::string &data) {
  reconnect();
  if (!connected) return -1;
  ssize_t nwrite = ::write(fd, data.c_str(), data.size());
  if (nwrite < 0 || (size_t)nwrite != data.size()) {
    connected = false;
    close(fd);
    return -1;
  }
  return nwrite;
}

void UdpsocketConnector::reconnect() {
  struct sockaddr_in sock;
  rapidjson::Document init,res;
  rapidjson::Value val;
  int rv;

  if (connected) return; // no point reconnecting if connected...
  connected = true;

  L<<Logger::Info<<"Reconnecting to backend" << std::endl;
  fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
     connected = false;
     L<<Logger::Error<<"Cannot create socket: " << strerror(errno) << std::endl;;
     return;
  }

  sock.sin_family = AF_INET;
  if (makeIPv4sockaddr(address, &sock)) {
     connected = false;
     L<<Logger::Error<<"Unable to create UDP socket to '"<<address<<"'"<<std::endl;
     return;
  }

  /*
  if (fcntl(fd, F_SETFL, O_NONBLOCK, &fd)) {
     connected = false;
     L<<Logger::Error<<"Cannot manipulate socket: " << strerror(errno) << std::endl;;
     close(fd);
     return;
  }
  */

  rv = connect(fd, reinterpret_cast<struct sockaddr*>(&sock), sizeof sock);

  if (rv != 0 && errno != EISCONN && errno != 0) {
     L<<Logger::Error<<"Cannot connect to socket: " << strerror(errno) << std::endl;
     close(fd);
     connected = false;
     return;
  }
  // send initialize

  init.SetObject();
  val = "initialize";
  init.AddMember("method",val, init.GetAllocator());
  val.SetObject();
  init.AddMember("parameters", val, init.GetAllocator());

  for(std::map<std::string,std::string>::iterator i = options.begin(); i != options.end(); i++) {
    val = i->second.c_str();
    init["parameters"].AddMember(i->first.c_str(), val, init.GetAllocator());
  } 

  this->send_message(init);
  if (this->recv_message(res) == false) {
     L<<Logger::Warning << "Failed to initialize backend" << std::endl;
     close(fd);
     this->connected = false;
  }
}
