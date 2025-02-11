import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import *


class KiwoomAPI(QAxWidget):

    def __init__(self):
        print(f'openapi __name__:{__name__}')
        super().__init__()
        self._create_kiwoom_api_instance()
        self._set_signal_slots()

    # ////////////////////////////////////////////////////////////////
    # //           login             ////////////////////////////////
    # //////////////////////////////////////////////////////////////
    def _create_kiwoom_api_instance(self):
        """ PyQt의 QAxWidget class를 사용해 API instance 생성
        """
        self.setControl('KHOPENAPI.KHOpenAPICtrl.1')

    def _set_signal_slots(self):
        try:
            self.OnEventConnect.connect(self._on_event_connect)
            print('_set_signal_slots')

        except Exception as e:
            ana_64bit = sys.maxsize > 2 ** 32
            if ana_64bit:
                print('******[Anaconda 64bit]******* 32bit 환경 필요')
            else:
                print(f'{e}')

    def comm_connect(self, **kwargs):
        """로그인 요청 (키움증권 로그인창 띄워줌. 자동로그인 설정시 바로 로그인 진행)
        OnEventConnect() 콜백
            :param kwargs:
            :return: 0: 로그인 요청 성공
        """
        lRet = self.dynamicCall('CommConnect()')
        self.event = QEventLoop()
        print("event loop 전")
        self.event.exec_()
        print("event loop 후")
        print("lRet",lRet)
        return lRet

    def _on_event_connect(self, nErrCode, **kwargs):
        """로그인 결과 수신
        로그인 성공시 [조건목록 요청] GetConditionLoad() 실행, 음수면 실패
            :param ErrCode: 0: 로그인 성공, 100: 사용자 정보교환 실패, 101: 서버접속 실패, 102: 버전처리 실패
            :param kwargs:
            :return:
        """
        if nErrCode == 0:
            print('[*----------[ 연결성공 ]----------*]')
        elif nErrCode == -100:
            print('********[사용자 정보교환 실패]********')
        elif nErrCode == -101:
            print('********   [서버접속 실패]   ********')
        elif nErrCode == -102:
            print('********   [버전처리 실패]   ********')
        self.event.exit()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    kiwoom = KiwoomAPI()
    a = kiwoom.comm_connect()
    print(a)
