#!/usr/bin/python

import json
import subprocess
import shlex
from StringIO import StringIO
import errno
import sys
import os
import io


keyring_base = '/tmp/cephtest-caps.keyring'

class UnexpectedReturn(Exception):
  def __init__(self, cmd, ret, expected, msg):
    if isinstance(cmd, list):
      self.cmd = ' '.join(cmd)
    else:
      assert isinstance(cmd, str) or isinstance(cmd, unicode), \
          'cmd needs to be either a list or a str'
      self.cmd = cmd
    self.cmd = str(self.cmd)
    self.ret = int(ret)
    self.expected = int(expected)
    self.msg = str(msg)

  def __str__(self):
    return repr('{c}: expected return {e}, got {r} ({o})'.format(
        c=self.cmd, e=self.expected, r=self.ret, o=self.msg))

def call(cmd):
  if isinstance(cmd, list):
    args = cmd
  elif isinstance(cmd, str) or isinstance(cmd, unicode):
    args = shlex.split(cmd)
  else:
    assert False, 'cmd is not a string/unicode nor a list!'

  print 'call: {0}'.format(args)
  proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  ret = proc.wait()

  return (ret, proc)

def expect(cmd, expected_ret):

  try:
    (r, p) = call(cmd)
  except ValueError as e:
    print >> sys.stderr, \
             'unable to run {c}: {err}'.format(c=repr(cmd), err=e.message)
    return errno.EINVAL

  assert r == p.returncode, \
      'wth? r was supposed to match returncode!'

  if r != expected_ret:
    raise UnexpectedReturn(repr(cmd), r, expected_ret, str(p.stderr.read()))

  return p

def expect_to_file(cmd, expected_ret, out_file, mode='a'):

  # Let the exception be propagated to the caller
  p = expect(cmd, expected_ret)
  assert p.returncode == expected_ret, \
      'expected result doesn\'t match and no exception was thrown!'

  with io.open(out_file, mode) as file:
    file.write(unicode(p.stdout.read()))

  return p

class Command:
  def __init__(self, cid, j):
    self.cid = cid[3:]
    self.perms = j['perm']
    self.module = j['module']

    self.sig = ''
    self.args = []
    for s in j['sig']:
      if not isinstance(s, dict):
        assert isinstance(s, str) or isinstance(s,unicode), \
            'malformatted signature cid {0}: {1}\n{2}'.format(cid,s,j)
        if len(self.sig) > 0:
          self.sig += ' '
        self.sig += s
      else:
        self.args.append(s)

  def __str__(self):
    return repr('command {0}: {1} (requires \'{2}\')'.format(self.cid,\
          self.sig, self.perms))

def get_commands():
  try:
    (r, p) = call('ceph --help --format=json')
  except ValueError as e:
    print >> sys.stderr, \
             'unable to obtain command descriptions: {err}'.format(err=e.message)
    sys.exit(errno.EINVAL)

  if r != 0:
    raise UnexpectedReturn(cmd, r, 0, p.stderr.read())

  cmds = []
  raw = json.loads(p.stdout.read())
  for (c_id, c) in raw.iteritems():
    if 'cli' not in c['avail']:
      print 'ignore unavailable command: {0}'.format(c['sig'])
      continue
    print c
    cmds.append(Command(c_id, c))
  return cmds


def test_by_perms(cmds):

  for cmd in cmds['r']:
    if len(cmd.args) == 0:
      p = expect_to_file(
          'ceph auth get-or-create client.all mon \'allow *\'', 0,
          '/tmp/k.client.all')
      p = expect_to_file(
          'ceph auth get-or-create client.none', 0,
          '/tmp/k.client.none')
      p = expect_to_file(
          'ceph auth get-or-create client.wrong mon \'allow command bar\'', 0,
          '/tmp/k.client.wrong')
      p = expect_to_file(
          'ceph auth get-or-create client.right mon \'allow command \"{c}\"\''.format(c=str(cmd.sig)), 0,
          '/tmp/k.client.right')
      p = expect_to_file(
          'ceph auth get-or-create client.ro mon \'allow r\'', 0,
          '/tmp/k.client.ro')
      p = expect_to_file(
          'ceph auth get-or-create client.wx mon \'allow wx\'', 0,
          '/tmp/k.client.wx')
      expect('ceph -k /tmp/k.client.all -n client.all ' + cmd.sig, 0)
      expect('ceph -k /tmp/k.client.none -n client.none ' + cmd.sig, errno.EACCES)
      expect('ceph -k /tmp/k.client.wrong -n client.wrong ' + cmd.sig, errno.EACCES)
      expect('ceph -k /tmp/k.client.right -n client.right ' + cmd.sig, 0)
      expect('ceph -k /tmp/k.client.ro -n client.ro ' + cmd.sig, 0)
      expect('ceph -k /tmp/k.client.wx -n client.wx ' + cmd.sig, errno.EACCES)

      expect('ceph auth del client.all', 0)
      os.unlink('/tmp/k.client.all')
      expect('ceph auth del client.none', 0)
      os.unlink('/tmp/k.client.none')
      expect('ceph auth del client.wrong', 0)
      os.unlink('/tmp/k.client.wrong')
      expect('ceph auth del client.right', 0)
      os.unlink('/tmp/k.client.right')
      expect('ceph auth del client.ro', 0)
      os.unlink('/tmp/k.client.ro')
      expect('ceph auth del client.wx', 0)
      os.unlink('/tmp/k.client.wx')
  return True

def gen_module_keyring(module):
  path = keyring_base + '.' + module

  types = [
      ('none', ''),
      ('wrong', 'allow service  {1}'),
      ('right', 'allow service {0} {1}'),
      ('ro', 'allow service {0} command')
      ]

  pass

def destroy_keyring(name):
  path = keyring_base + '.' + name
  if not os.path.exists(path):
    raise Exception('oops! cannot remove inexistent keyring {0}'.format(path))

  # grab all client entities from the keyring
  entities = [m.group(1) for m in [re.match(r'\[client\.(.*)\]', l)
                for l in [str(line.strip())
                  for line in io.open(path,'r')]] if m is not None]

  # clean up and make sure each entity is gone
  for e in entities:
    expect('ceph auth del client.{0}'.format(e), 0)
    expect('ceph auth get client.{0}'.format(e), errno.EINVAL)

  # remove keyring
  os.unlink(path)

  return True

def test_by_module(cmds):

  for module in cmds.iterkeys():
    gen_module_keyring(module)

    destroy_keyring(module)
  return True

def test_basic_auth():
  # make sure we can successfully add/del entities, change their caps
  # and import/export keyrings.

  expect('ceph auth add client.basicauth', 0)
  expect('ceph auth caps client.basicauth mon \'allow *\'', 0)
  expect('ceph auth add client.basicauth', errno.EEXIST)


def main():

  test_basic_auth()


  cmds = get_commands()

  cmd_module = {}
  cmd_perms = {'r':[], 'rw':[] }

  for c in cmds:
    cmd_perms[c.perms].append(c)

    if c.module not in cmd_module:
      cmd_module[c.module] = {}
    if c.perms not in cmd_module[c.module]:
      cmd_module[c.module][c.perms] = []
    cmd_module[c.module][c.perms].append(c)

  success = test_by_perms(cmd_perms)
  if not success:
    print >> sys.stderr, \
        'error testing by permission'

  success = test_by_module(cmd_module)
  if not success:
    print >> sys.stderr, \
        'error testing by module'

if __name__ == '__main__':
  main()

