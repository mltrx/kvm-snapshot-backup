#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import fcntl
import glob
import shutil
from datetime import datetime
import libvirt
import logging
import os
import re
import shlex
import subprocess
import sys
from xml.etree import ElementTree
from collections import namedtuple


class Domain(object):
    def __init__(self, libvirt_domain: libvirt.virDomain):
        self.libvirt_domain = libvirt_domain
        self.name = libvirt_domain.name()
        self.libvirt_snapshot = None

    def get_disks(self):
        """ Gets all domain disk as namedtuple('DiskInfo', ['device', 'file', 'format']) """
        # root node
        root = ElementTree.fromstring(self.libvirt_domain.XMLDesc())

        # search <disk type='file' device='disk'> entries
        disks = root.findall("./devices/disk[@device='disk']")

        # for every disk get drivers, sources and targets
        drivers = [disk.find("driver").attrib for disk in disks]
        sources = [disk.find("source").attrib for disk in disks]
        targets = [disk.find("target").attrib for disk in disks]

        # iterate drivers, sources and targets
        if len(drivers) != len(sources) != len(targets):
            raise RuntimeError("Drivers, sources and targets lengths are different %s:%s:%s" % (
                len(drivers), len(sources), len(targets)))

        disk_info = namedtuple('DiskInfo', ['device', 'file', 'format'])

        # all disks info
        disks_info = []

        for i in range(len(sources)):
            disks_info.append(disk_info(targets[i]["dev"], sources[i]["file"], drivers[i]["type"]))

        return disks_info

    def create_snapshot_xml(self):
        """ Creates snapshot XML """
        snapshot_name = datetime.now().strftime("%Y%m%d_%H%M%S")

        disk_specs = []
        domain_disks = self.get_disks()

        for disk in domain_disks:
            disk_basedir = os.path.dirname(disk.file)
            disk_specs += ["--diskspec %s,file=\"%s/%s_%s-%s.%s\"" % (
                disk.device, disk_basedir, self.name, disk.device, snapshot_name, disk.format)]

        if not disk_specs:
            raise RuntimeError("Wrong disk devices specified? Available devices: %s" % domain_disks)

        snapshot_create_cmd = (
            "virsh snapshot-create-as --domain {domain_name} {snapshot_name} {disk_specs}"
            " --disk-only --atomic --quiesce --print-xml").format(
            domain_name=self.name, snapshot_name=snapshot_name, disk_specs=" ".join(disk_specs))
        logging.debug("Executing: '%s'" % snapshot_create_cmd)

        snapshot_create_cmds = shlex.split(snapshot_create_cmd)

        create_xml = subprocess.Popen(snapshot_create_cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)

        snapshot_xml = create_xml.stdout.read()

        status = create_xml.wait()

        if status != 0:
            logging.error("Error for '%s': %s" % (snapshot_create_cmd, create_xml.stderr.read()))
            logging.critical("{exe} returned {status} state".format(exe=snapshot_create_cmds[0], status=status))
            raise Exception("snapshot-create-as didn't work properly")

        return snapshot_xml

    def create_snapshot(self):
        """ Creates domain snapshot """
        logging.debug("Creating snapshot XML")
        snapshot_xml = self.create_snapshot_xml()

        logging.info("Creating snapshot based on snapshot XML")
        self.libvirt_snapshot = self.libvirt_domain.snapshotCreateXML(
            snapshot_xml.decode('utf-8'),
            flags=sum(
                [libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_DISK_ONLY,
                 libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC,
                 # libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_QUIESCE,
                 libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_NO_METADATA]))
        return self.libvirt_snapshot

    def backup_incremental(self, backup_dir: str):
        """ Makes incremental backup with snapshots (with check that we have all backups files).
        It creates snapshot, backup snapshot backing file and checks that every parent backing files are in backups.
        """
        # create snapshot if not exists
        if self.libvirt_snapshot is None:
            self.create_snapshot()

        backup_domain_dir = self.get_backup_domain_dir(backup_dir)

        # create backup domain directory if not exists
        if not os.path.exists(backup_domain_dir) and not os.path.isdir(backup_domain_dir):
            logging.info("Creating directory '%s'" % backup_domain_dir)
            os.mkdir(backup_domain_dir)

        # for every disk
        for disk in self.get_disks():
            # get current backing file from disk_info
            backing_file = DiskImageHelper.get_backing_file(disk.file)
            # while it is a backing file for every snapshot disk
            while backing_file is not None:
                # backup file
                backup_file = os.path.join(backup_domain_dir, os.path.basename(backing_file))
                # do backup if not exists
                backing_file_copy_result = False
                if os.path.isfile(backup_file) is False:
                    # copy
                    logging.info("Copying '%s' to '%s'" % (backing_file, backup_file))
                    shutil.copy2(backing_file, backup_file)
                    backing_file_copy_result = True

                # set parent backing file
                backing_file = DiskImageHelper.get_backing_file(backing_file)
                # set valid backing file for backup_file after copy
                if backing_file_copy_result and backing_file:
                    DiskImageHelper.set_backing_file(os.path.basename(backing_file), backup_file)

        # backup domain XML
        backup_xml_file = "%s/%s.xml" % (backup_domain_dir, self.name)
        logging.info("Creating domain XML backup '%s" % backup_xml_file)
        backup_xml_fo = open(backup_xml_file, 'w')
        backup_xml_fo.write(self.libvirt_domain.XMLDesc())
        backup_xml_fo.close()

    def get_backup_domain_dir(self, backup_dir):
        """ Gets full backup domain dir """
        # prepare backup domain directory path
        backup_domain_dir = os.path.join(backup_dir, self.name)
        return backup_domain_dir

    def merge_snapshot(self):
        """ Merges base to snapshot and removes old disk files """
        disks = self.get_disks()
        disk_files_tree = []
        for disk in disks:
            disk_files_tree += (DiskImageHelper.get_backing_files_tree(disk.file))
            merge_snapshot_cmd = "virsh blockpull --domain {domain_name} {disk_path} --wait".format(
                domain_name=self.name, disk_path=disk.file)

            logging.debug("Executing: '%s'" % merge_snapshot_cmd)
            logging.info("Merging base to new snapshot for '%s' device" % disk.device)

            # launch command
            merge_snapshot_cmds = shlex.split(merge_snapshot_cmd)
            merge_snapshot = subprocess.Popen(merge_snapshot_cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                              shell=False)

            # wait to terminate
            status = merge_snapshot.wait()

            if status != 0:
                logging.error("Error for '%s': %s" % (merge_snapshot_cmd, merge_snapshot.stderr.read()))
                logging.critical("{exe} returned {status} state".format(exe=merge_snapshot_cmds[0], status=status))
                raise Exception("blockpull didn't work properly")

        current_disk_files = [disk.file for disk in self.get_disks()]

        # remove old disk device files without current ones
        for file in [disk_file_tree for disk_file_tree in disk_files_tree if disk_file_tree not in current_disk_files]:
            logging.info("Removing old disk file: '%s'" % file)
            os.remove(file)

    def backup_rotate_daily(self, backup_dir: str, rotate: int):
        """ Rotates domain backup disk files in 'backup_dir' leaving the last 'rotate' backups """
        if rotate < 1:
            raise Exception("Rotate should be more than 0")
        backup_domain_dir = self.get_backup_domain_dir(backup_dir)
        # for every file in directory group backups
        for disk in self.get_disks():
            grouped_files = []
            backup_files = glob.glob(
                os.path.join(backup_domain_dir, "%s_%s-*.%s" % (self.name, disk.device, disk.format)))
            backup_files.sort(key=os.path.getmtime, reverse=True)
            backing_file = None
            for backup_file in backup_files:
                if backing_file is None:
                    grouped_files.append([])
                grouped_files[-1].append(backup_file)
                backing_file = DiskImageHelper.get_backing_file(backup_file)
            logging.debug("Grouped backup files %s" % grouped_files)
            grouped_files_to_remove = grouped_files[rotate:]
            logging.debug("Groups to remove %s" % grouped_files_to_remove)
            for group in grouped_files_to_remove:
                for file in group:
                    logging.info("Removing old backup disk file: '%s'" % file)
                    os.remove(file)


class DiskImageHelper(object):
    @staticmethod
    def get_backing_file(file: str):
        """ Gets backing file for disk image """
        get_backing_file_cmd = "qemu-img info %s" % file
        logging.debug("Executing: '%s'" % get_backing_file_cmd)
        out = subprocess.check_output(shlex.split(get_backing_file_cmd))
        lines = out.decode('utf-8').split('\n')
        for line in lines:
            if re.search("backing file:", line):
                return line.strip().split()[2]
        return None

    @staticmethod
    def get_backing_files_tree(file: str):
        """ Gets all backing files (snapshot tree) for disk image """
        backing_files = []
        backing_file = DiskImageHelper.get_backing_file(file)
        while backing_file is not None:
            backing_files.append(backing_file)
            backing_file = DiskImageHelper.get_backing_file(backing_file)
        return backing_files

    @staticmethod
    def set_backing_file(backing_file: str, file: str):
        """ Sets backing file for disk image """
        set_backing_file_cmd = "qemu-img rebase -u -b %s %s" % (backing_file, file)
        logging.debug("Executing: '%s'" % set_backing_file_cmd)
        subprocess.check_output(shlex.split(set_backing_file_cmd))


script_name = os.path.basename(sys.argv[0][:-3])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    backup_parser = subparsers.add_parser('backup')
    backup_parser.add_argument('-a', '--action', action="store_const", const="backup", default="backup")
    backup_parser.add_argument('-d', '--domain', required=True)
    backup_parser.add_argument('-b', '--backup-dir', dest='backup_dir', required=True)
    merge_parser = subparsers.add_parser('merge')
    merge_parser.add_argument('-a', '--action', action="store_const", const="merge", default="merge")
    merge_parser.add_argument('-d', '--domain', required=True)
    rotate_parser = subparsers.add_parser('rotate')
    rotate_parser.add_argument('-a', '--action', action="store_const", const="backup", default="rotate")
    rotate_parser.add_argument('-r', '--rotate', default=1, type=int)
    rotate_parser.add_argument('-d', '--domain', required=True)
    rotate_parser.add_argument('-b', '--backup-dir', dest='backup_dir', required=True)
    parser.add_argument('-v', dest='verbose', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        type=str)

    command_args = parser.parse_args()

    if not any(vars(command_args).values()) or vars(command_args) == {"verbose": "INFO"}:
        parser.print_usage()
        exit()

    try:
        pid_file = '/tmp/kvm_snapshot_backup.lock'
        fp = open(pid_file, 'w')

        # lock script instance, only one instance should run
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)

        logging.basicConfig(format='%(levelname)s: %(message)s', level=getattr(logging, command_args.verbose.upper()))

        logging.debug("START")

        logging.debug("Opening libvirt connection to qemu")
        app_libvirt_conn = libvirt.open("qemu:///system")

        app_domain = Domain(app_libvirt_conn.lookupByName(command_args.domain))

        if command_args.action == "backup":
            app_domain.backup_incremental(command_args.backup_dir)
        elif command_args.action == "merge":
            app_domain.merge_snapshot()
        elif command_args.action == "rotate":
            app_domain.backup_rotate_daily(command_args.backup_dir, command_args.rotate)

        app_libvirt_conn.close()

    except IOError as ioe:
        logging.error(str(ioe))
    except RuntimeError as re:
        logging.error(str(re))

    logging.debug("STOP")
