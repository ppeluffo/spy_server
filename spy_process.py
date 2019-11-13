#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 19:55:43 2019

@author: pablo
"""

import os
import configparser
import sys
import glob
import time
import re
import spy_log as LOG
from spy_bd_general import BD
import ast
import psutil
import signal

MAXPROCESS = 5
# -----------------------------------------------------------------------------
Config = configparser.ConfigParser()
Config.read('spy.conf')
#
console = ast.literal_eval( Config['MODO']['consola'] )
#-----------------------------------------------------------------------------

def format_fecha_hora(fecha, hora):
    '''
    Funcion auxiliar que toma la fecha y hora de los argumentos y
	las retorna en formato DATE de la BD.
    121122,180054 ==> 2012-11-22 18:00:54
    '''
    lfecha = [(fecha[i:i + 2]) for i in range(0, len(fecha), 2)]
    lhora = [(hora[i:i + 2]) for i in range(0, len(hora), 2)]
    timestamp = '%s%s-%s-%s %s:%s:%s' % ( lfecha[0], lfecha[1], lfecha[2], lfecha[3], lhora[0], lhora[1], lhora[2] )
    return(timestamp)


def move_file_to_error_dir(file):
    # Muevo el archivo al error.
    LOG.log(module=__name__, server='process', function='move_file_to_error_dir', dlgid='PROC00', msg='ERROR: FILE {}'.format(file))
    dirname, filename = os.path.split(file)
    try:
        errdirname = Config['PROCESS']['process_err_path']
        errfile = os.path.join(errdirname, filename)
        LOG.log(module=__name__, server='process', function='move_file_to_error_dir', level='SELECT', dlgid='PROC00', console=console, msg='errfile={}'.format(errfile))
        os.rename(file, errfile)
    except Exception as err_var:
        LOG.log(module=__name__, server='process', function='move_file_to_error_dir', console=console, dlgid='PROC00', msg='ERROR: [{0}] no puedo pasar a err [{1}]'.format(file, err_var))
        LOG.log(module=__name__, server='process', function='move_file_to_error_dir', dlgid='PROC00', msg='ERROR: EXCEPTION {}'.format(err_var))
    return


def move_file_to_bkup_dir(file):
    # Muevo el archivo al backup.
    dirname, filename = os.path.split(file)
    try:
        bkdirname = Config['PROCESS']['process_bk_path']
        bkfile = os.path.join(bkdirname, filename)
        LOG.log(module=__name__, server='process', function='move_file_to_bkup_dir', level='SELECT', dlgid='PROC00', console=console, msg='bkfile={}'.format(bkfile))
        os.rename(file, bkfile)
    except Exception as err_var:
        LOG.log(module=__name__, server='process', function='move_file_to_bkup_dir', console=console, dlgid='PROC00', msg='ERROR: [{0}] no puedo pasar a bk [{1}]'.format(file, err_var))
        LOG.log(module=__name__, server='process', function='move_file_to_bkup_dir', dlgid='PROC00', msg='ERROR: EXCEPTION {}'.format(err_var))
    return


def move_file_to_ute_dir(file):
    # Muevo el archivo al directorio 'ute' para ser procesados luego por DLGDB.
    dirname, filename = os.path.split(file)
    try:
        utedirname = Config['PROCESS']['process_ute_path']
        utefile = os.path.join(utedirname, filename)
        LOG.log(module=__name__, server='process', function='move_file_to_ute_dir', level='SELECT', dlgid='PROC00', console=console, msg='utefile={}'.format(utefile))
        os.rename(file, utefile)
    except Exception as err_var:
        LOG.log(module=__name__, server='process', function='move_file_to_ute_dir', console=console, dlgid='PROC00', msg='ERROR: [{0}] no puedo pasar a ute [{1}]'.format(file, err_var))
        LOG.log(module=__name__, server='process', function='move_file_to_ute_dir', dlgid='PROC00', msg='ERROR: EXCEPTION {}'.format(err_var))
    return


def process_line( line, bd):
    '''
    Recibo una linea, la parseo y dejo los campos en un diccionario
    Paso este diccionario a la BD para que la inserte.
    '''
    LOG.log(module=__name__, server='process', function='process_line', level='SELECT', dlgid='PROC00', console=console, msg='line={}'.format(line))
    line = line.rstrip('\n|\r|\t')
    fields = re.split(',', line )
    d = dict()
    d['timestamp'] = format_fecha_hora(fields[0], fields[1])
    d['RCVDLINE'] = line
    for field in fields[2:]:
        key, value = re.split('=', field)
        d[key] = value

    #for key in d:
    #    LOG.log(module=__name__, server='process', function='process_line', level='SELECT', dlgid='PROC00', msg='key={0}, val={1}'.format(key,d[key]))

    if not bd.insert_data_line(d):
        return False

    if not bd.insert_data_online(d):
        return False

    return True


def process_file(file):
    '''
    Recibo el nombre de un archivo el cual abro y voy leyendo c/linea
    y procesandola
    Al final lo muevo al directorio de backups
    c/archivo puede corresponder a un datalogger distinto por lo tanto el datasource puede ser
    distinto.
    Debo entonces resetear el datasource antes de procesar c/archivo
    '''
    dirname, filename = os.path.split(file)
    LOG.log(module=__name__, server='process', function='process_file', level='SELECT', dlgid='PROC00', msg='file={}'.format(filename))
    dlgid, *res = re.split('_', filename)
    LOG.log(module=__name__, server='process', function='process_file', level='SELECT', dlgid='PROC00', msg='DLG={}'.format(dlgid))
    bd = BD(modo=Config['MODO']['modo'], dlgid=dlgid, server='process')
    if bd.datasource == 'DS_ERROR':
        LOG.log(module=__name__, server='process', function='process_file', level='SELECT', dlgid='PROC00',msg='ERROR: DS not found !!')
        move_file_to_error_dir(file)
        return

    print('Im a child with pid {0} and FILE {1}'.format(os.getpid(), file))
    with open(file) as myfile:
        for line in myfile: 
            if not process_line( line, bd):
                move_file_to_error_dir(file)
                return

    del bd
    if Config['SITE']['site'] == 'ute':
        move_file_to_ute_dir(file)
    else:
        move_file_to_bkup_dir(file)
    return


if __name__ == '__main__':

    # Lo primero es configurar el logger y desconocer las señales SIGCHILD
    # de los child que cree para poder despegarme como demonio
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    LOG.config_logger()
    dirname = Config['PROCESS']['process_rx_path']
    LOG.log(module=__name__, server='process', function='main', console=console, dlgid='PROC00', msg='SERVER: dirname={}'.format(dirname))
    pid_list = list()
    #
    # A efectos de testing en modo consola corro como ./spy_process.py DEBUG
    if len(sys.argv) > 1 and sys.argv[1] == 'DEBUG':
        print('spy_process en modo DEBUG !!!')
        while True:
            file_list = glob.glob(dirname + '/*.dat')
            print ('File List: {}'.format(len(file_list)))
            for file in file_list:
                LOG.log(module=__name__, server='process', function='main', level='SELECT', dlgid='PROC00', console=console,  msg='File {}'.format(file))
                process_file(file)

            time.sleep(60)

    # Modo normal
    while True:
        file_list = glob.glob(dirname + '/*.dat')
        for file in file_list:
            # Mientras halla lugar en la lista, proceso archivos
            if len(pid_list) < MAXPROCESS:
                # Creo un child
                pid = os.fork()
                if pid == 0:
                    process_file(file)
                    sys.exit(0)
                else:
                    pid_list.append(pid)
                    LOG.log(module=__name__, server='process', function='main', dlgid='PROC00', msg='SERVER: append child {}'.format(pid))
                    print('Server: append child {}'.format(pid))

            # No queda espacio: Espero que se vaya haciendo lugar
            while len(pid_list) == MAXPROCESS:
                LOG.log(module=__name__, server='process', function='main', dlgid='PROC00', msg='SERVER: List FULL: {}'.format(pid_list))
                print('List FULL: {}'.format(pid_list))
                time.sleep(3)
                # Veo si alguno termino y lo saco de la lista para que quede espacio para otro
                for pid in pid_list:
                    if not psutil.pid_exists(pid):
                        pid_list.remove(pid)
                        LOG.log(module=__name__, server='process', function='main', dlgid='PROC00',msg='SERVER: remove pid {}'.format(pid))
                        print('Server remove pid {}'.format(pid))

        # No hay mas archivos por ahora: espero
        LOG.log(module=__name__, server='process', function='main', dlgid='PROC00',msg='SERVER: No hay mas archivos: espero')
        print('Server No hay mas archivos: espero')
        time.sleep(60)
