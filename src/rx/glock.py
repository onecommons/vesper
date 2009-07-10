#this is derived from PEAK's running.lockfiles, specifically:
#http://cvs.eby-sarna.com/PEAK/src/peak/running/lockfiles.py?rev=1.21
#but modified to enable the lock to be reentrant
"""Lockfiles for inter-process communication

    These are used for synchronization between processes, unlike
    thread.LockType locks.  The common use is non-blocking lock attempts.
    For convenience and in order to reduce confusion with the (somewhat odd)
    thread lock interface, these locks have a different interface.

    attempt()   try to obtain the lock, return boolean success
    obtain()    wait to obtain the lock, returns None
    release()   release an obtained lock, returns None
    locked()    returns True if any thread IN THIS PROCESS
                has obtained the lock, else False

    Currently, only Unix-ish and Windows platforms supported; if your platform
    isn't supported, not even the 'LockFile' class will be available from this
    module.  For Windows, the 'msvcrt' module must be available (it is in the
    standard Python 2.2.1 binary distribution for Windows).

    This module also exports a 'NullLockFile' class, for use when locking is
    not needed, but an object with a locking interface is nonetheless required.
    'NullLockFile' can also be used as a substitute for a thread lock, if you
    prefer this locking interface over the standard Python one.
"""
import os, errno, time, threading, sys

#exception classes leftover from the old glock implementation that was based on
#http://rgruet.free.fr/rgutils/doc/public/rgutils.glock.GlobalLock-class.html
class GlobalLockError(Exception):
    ''' Error raised by the glock module.
    '''

class NotOwner(GlobalLockError):
    ''' Attempt to release somebody else's lock.
    '''

class LockFileBase(object):
    """Common base for lockfiles"""

    def __init__(self, fn):
        self.fn = os.path.abspath(fn)
        self._lock = threading.RLock()#thread.allocate_lock()
        #whether the inter-process lock is locked (not the thread lock)
        self._locked = False
        self.count = 0

    def attempt(self):
        if self._lock.acquire(False):
            r = False
            try:
                #only set the inter-process lock once per process
                #(for WinFlockFile)
                r = self._locked or self.do_acquire(False)
                if r:
                    self.count += 1
            finally:
                if not r:
                    self._lock.release()
            return r
        else:
            return False

    def obtain(self):
        self._lock.acquire()
        r = False
        try:
            r = self._locked or self.do_acquire(True)
            if r:
                self.count += 1            
        finally:
            if not r :
                self._lock.release()
        if not r:
            raise GlobalLockError, "lock obtain shouldn't fail!"

    def release(self):
        if not self.count:
            raise NotOwner
        self.count -= 1        
        if not self.count and self._locked:
            self.do_release()
        self._lock.release()
            
    def locked(self):
        return self._locked

class NullLockFile(LockFileBase):

    """Pseudo-LockFile (locks only for threads in this process)"""

    def do_acquire(self, waitflag=False):
        self._locked = True
        return True

    def do_release(self):
        self._locked = False

LockFile=NullLockFile #default to this if nothing else is available

### Posix-y lockfiles ###

try:
    import posix
    from posix import O_EXCL, O_CREAT, O_RDWR
except ImportError:
    posix=None

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

if posix and fcntl:
    class FLockFile(LockFileBase):
        """
        flock(3)-based locks.

        Wins:

          o Locks do not survive crashes of either the system or the
            application.

          o Waiting for a lock is handled by the kernel and doesn't require
            polling

          o Potentially compatible with NFS or other shared filesystem
            _if_ you trust their lockd (or equivalent) implemenation.
            Note that this is a *big* if!

          o No false positives on stale locks

        Loses:

          o Leaves lockfiles around, since unlink would cause a race.
        """
        fd = None
        
        def do_acquire(self, waitflag=False):
            locked = False

            if waitflag:
                blockflag = 0
            else:
                blockflag = fcntl.LOCK_NB

            self.fd = posix.open(self.fn, O_CREAT | O_RDWR, 0600)
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX|blockflag)
                # locked it
                try:
                    posix.ftruncate(self.fd, 0)
                    posix.write(self.fd, `os.getpid()` + '\n')
                    locked = True
                except:
                    self.do_release()
                    raise
            except IOError, x:
                if x.errno == errno.EWOULDBLOCK:
                    # failed to lock
                    posix.close(self.fd)
                    del self.fd
                else:
                    raise

            self._locked = locked

            return locked

        def do_release(self):
            if self.fd is not None:
                posix.ftruncate(self.fd, 0)
                fcntl.flock(self.fd, fcntl.LOCK_UN)
                posix.close(self.fd)
                self.fd = None
                self._locked = False
                
    LockFile=FLockFile
                
if posix:
    def pid_exists(pid):
        """Is there a process with PID pid?"""
        if pid < 0:
            return False

        exist = False
        try:
            os.kill(pid, 0)
            exist = 1
        except OSError, x:
            if x.errno != errno.ESRCH:
                raise
        return exist

    def check_lock(fn):
        """Check the validity of an existing lock file
        Reads the PID out of the lock and check if that process exists"""
        try:
            f = open(fn, 'r')
            pid = f.read().strip()
            pid = int(pid)
            f.close()
            return pid_exists(pid)
        except:
            raise
            return 1 # be conservative

    def make_tempfile(fn, pid):
        tfn = os.path.join(os.path.dirname(fn), 'shlock%d.tmp' % pid)

        errcount = 1000
        while 1:
            try:
                fd = posix.open(tfn, O_EXCL | O_CREAT | O_RDWR, 0600)
                posix.write(fd, '%d\n' % pid)
                posix.close(fd)

                return tfn
            except OSError, x:
                if (errcount > 0) and (x.errno == errno.EEXIST):
                    os.unlink(tfn)
                    errcount = errcount - 1
                else:
                    raise

    class SHLockFile(LockFileBase):
        """HoneyDanBer/NNTP/shlock(1)-style locking

        Two bigs wins to this algorithm:

          o Locks do not survive crashes of either the system or the
            application by any appreciable period of time.

          o No clean up to do if the system or application crashes.

        Loses:

          o In the off chance that another process comes along with
            the same pid, we can get a false positive for lock validity.

          o Not compatible with NFS or any shared filesystem
            (due to disjoint PID spaces)

          o Waiting for lock must be implemented by polling"""

        def do_acquire(self, waitflag):
            if waitflag:
                sleep = 1
                locked = self.do_acquire(False)

                while not locked:
                    time.sleep(sleep)
                    sleep = min(sleep + 1, 15)
                    locked = self.do_acquire(False)

                return locked
            else:
                tfn = make_tempfile(self.fn, os.getpid())
                while 1:
                    try:
                        os.link(tfn,self.fn)
                        os.unlink(tfn)
                        self._locked = True
                        return True

                    except OSError, x:
                        if x.errno == errno.EEXIST:
                            if check_lock(self.fn):
                                os.unlink(tfn)
                                self._locked = False
                                return False
                            else:
                                # nuke invalid lock file, and try to lock again
                                os.unlink(self.fn)
                        else:
                            os.unlink(tfn)
                            raise

        def do_release(self):
            os.unlink(self.fn)
            self._locked = False

    if sys.platform != 'cygwin':
        #default to this instead of FLockFile for unix-like system
        #except cygwin, which doesn't support os.link()
        LockFile=SHLockFile 
    
if msvcrt:
    class WinFLockFile(LockFileBase):
        """Like FLockFile, but for Windows"""
        #http://rgruet.free.fr/rgutils/doc/public/rgutils.glock.GlobalLock-class.html
        #claims that "Can't use std module msvcrt.locking(), because global lock is OK,
        #but blocks also for 2 calls from the same thread!". I don't see that behavior
        #myself but just in case we only call msvcrt.locking() once per process
        f = None

        def do_acquire(self, waitflag=False):            
            if waitflag:
                sleep = 1
                locked = self.do_acquire(False)

                while not locked:
                    time.sleep(sleep)
                    sleep = min(sleep + 1, 15)
                    locked = self.do_acquire(False)
                return locked

            locked = False

            self.f = open(self.fn, 'a')
            try:
                msvcrt.locking(self.f.fileno(), msvcrt.LK_NBLCK, 1)
                try:
                    self.f.write(`os.getpid()` + '\n')  # informational only
                    self.f.seek(0)  # lock is at offset 0, so go back there
                    locked = True
                except:
                    self.do_release()
                    raise

            except IOError, x:
                if x.errno == errno.EACCES:
                    self.f.close()
                    del self.f
                else:
                    raise

            self._locked = locked
            return locked

        def do_release(self):
            if self.f:
                msvcrt.locking(self.f.fileno(), msvcrt.LK_UNLCK, 1)
                self.f.close()
                self.f=None
                self._locked = False
                
    LockFile=WinFLockFile        

class LockGetter:
    '''
    Helper class enabling the "resource acquisition is initialization" pattern.
    The constructor acquires the resource (the lock) and the destructor releases it
    (e.g. when garage collected).
    But don't rely on del or the garbage collector! -- instead call release().
    '''
    def __init__(self, globalLock):
        self.globalLock = globalLock
        self.globalLock.obtain()
        
    def __del__(self):
        #print '__del__ called' ##
        if self.globalLock:
            self.globalLock.release()
        
    def release(self):
        self.globalLock.release()
        self.globalLock = None
