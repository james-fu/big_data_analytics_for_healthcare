When run `vagrant up` it may display
> The SSH connection was unexpectedly closed by the remote end. This usually indicates that SSH within the guest machine was unable to properly start up. Please boot the VM in GUI mode to check whether it is booting properly.

possible readon is related to `nfsd`, resolve it by
```bash
vagrant halt
sudo /sbin/nfsd restart
vagrant up
```