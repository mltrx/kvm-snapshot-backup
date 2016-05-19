kvm-snapshot-backup
===================

Creates KVM external disk incremental snapshot backups.

Requirements
------------
- python 3.x
- libvirt-python
- virsh

Usage
-----

1. Incremental backup
   <pre># ./kvm_snapshot_backup.py backup -d vm1 -b /backups/vm</pre>
   <p>It creates domain snapshot and copy backing file of current disk to 'backup_dir' (-b).<br/>
   After this it validates all backups disk files are consistent.</p>
   <p>You can backup your domain disk files and merge complete base to current snapshot weekly, monthly or whatever.</p>

2. Merge base to current snapshot
   <pre># ./kvm_snapshot_backup.py merge -d vm1</pre>
   <p>Merges complete base to snapshot and removes old disk files (backing file tree). Finally you have one file for every disk device in domain.<br/>It uses virsh blockpull mechanism.</p>

2. Rotate backup disk files
   <pre># ./kvm_snapshot_backup.py rotate -d vm1 -b /backups/vm [-r 1]</pre>
   <p>Rotates domain backup disk files in 'backup_dir' (-b) leaving the last X (-r) backups, defaults to 1.</p>
