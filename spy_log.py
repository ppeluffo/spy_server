#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 09:19:01 2019

@author: pablo
"""

import logging
import logging.handlers
import ast
from spy import Config
#import spy

def config_logger():
    # logging.basicConfig(filename='log1.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s', level = logging.DEBUG, datefmt = '%d/%m/%Y %H:%M:%S' )
    logging.basicConfig(level=logging.DEBUG)

    # formatter = logging.Formatter('SPX %(asctime)s  [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %T')
    formatter = logging.Formatter('SPY_r4 [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.handlers.SysLogHandler('/dev/log')
    handler.setFormatter(formatter)
    # Creo un logger derivado del root para usarlo en los modulos
    logger1 = logging.getLogger()
    # Le leo el handler de consola para deshabilitarlo
    lhStdout = logger1.handlers[0]  # stdout is the only handler initially
    logger1.removeHandler(lhStdout)
    # y le agrego el handler del syslog.
    logger1.addHandler(handler)
    # Creo ahora un logger child local.
    LOG = logging.getLogger('spy')
    LOG.addHandler(handler)
    #return LOG

# ------------------------------------------------------------------------------

def log(module, function, server = 'comms', msg='', dlgid='00000', console=False, level='INFO'):
    '''
    Se encarga de mandar la logfile el mensaje.
    Si el level es SELECT, dependiendo del dlgid se muestra o no
    Si console es ON se hace un print del mensaje
    '''
    dlg_list = Config['SELECT']['list_dlg']
    list_dlg = ast.literal_eval( dlg_list )

    if level == 'SELECT':
        if dlgid in list_dlg:
            logging.info('[{0}][{1}][{2}][{3}]: [{4}]'.format( server,dlgid, module,function, msg))
            if console == True:
                print('Process [{0}][{1}][{2}][{3}]'.format( module,function,dlgid,msg))
            return
        else:
            return

    logging.info('[{0}][{1}][{2}][{3}]: [{4}]'.format( server, dlgid, module,function,msg))
    if console:
        print('Process [{0}][{1}][{2}][{3}]'.format( module, function, dlgid, msg))

    return


if __name__ == '__main__':
    #from spy import Config
    list_dlg = ast.literal_eval(Config['SELECT']['list_dlg'])
