#!/usr/bin/python3 -u
# ------------------------------------------------------------------------------
class BDGDA:

    def __init__(self, modo='local'):
        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False
        self.url = 'mysql://root:spymovil@localhost/GDA'
        return

    def connect(self, tag='GDA'):
        """
        Retorna True/False si es posible generar una conexion a la bd GDA
        """
        print('DEBUG_1: {0}[{1}]'.format(tag, self.url))
        if self.connected:
            print('DEBUG_2: {0}'.format(tag))
            return self.connected

        self.connected = True
        print('DEBUG_5 : {0}'.format(self.connected))
        return self.connected

    def test_try(self):

        Var1 = False
        if ( 'Var1' in locals()):
            print ('A) Var1 local')
        if ( 'Var1' in globals()):
            print ('A) Var1 global')
        print( 'A: Var1={}'.format(Var1))
        try:
            Var1 = True
            if ('Var1' in locals()):
                print('B) Var1 local')
            if ('Var1' in globals()):
                print('B) Var1 global')
            print('Inside Try: {}'.format(Var1))
            #return Var1
        except:
            Var1 = False
            if ('Var1' in locals()):
                print('C) Var1 local')
            if ('Var1' in globals()):
                print('C) Var1 global')
            print('Inside Except: {}'.format(Var1))
            #return Var1
        finally:
            Var1 = True
            #return Var1
            if ('Var1' in locals()):
                print('D) Var1 local')
            if ('Var1' in globals()):
                print('D) Var1 global')
            print('Inside Finally: {}'.format(Var1))

        print('Out block: {}'.format(Var1))
        print(Var1)
        return Var1

    def read_dlg_conf(self, dlgid, tag='GDA'):
        print('start')
        a = self.connect(tag)
        print('A=%s' % a )
        print('DEBUG A {}'.format( a))


class BDGDA_TAHONA(BDGDA):

    def __init__(self, modo='local'):

        self.datasource = ''
        self.engine = ''
        self.conn = ''
        self.connected = False
        self.url = 'mysql://root:spymovil@localhost/GDA_LA_TAHONA'
        return

    def connect(self, tag='GDA_TAHONA'):
        BDGDA.connect(self, tag=tag )

    def read_dlg_conf(self, dlgid, tag='GDA_TAHONA'):
        BDGDA.read_dlg_conf(self, dlgid, tag=tag)


if __name__ == '__main__':
    bd = BDGDA_TAHONA()
    res = bd.connect('TH')
    #res = bd.test_try()
    print(res)
    print('RES=[{}]'.format(res))