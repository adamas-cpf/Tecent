import socket
import threading
from datetime import datetime

class TCPServer:
    def __init__(self, host='0.0.0.0', port=8888):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 允许端口复用
        self.clients = {}  # 记录客户端连接（{conn: 地址}）

    def start(self):
        """启动服务端"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"服务端启动，监听 {self.host}:{self.port}（支持多客户端长连接）")
        
        # 循环接受新客户端连接
        while True:
            conn, addr = self.server_socket.accept()
            print(f"[{datetime.now().isoformat()}] 新客户端连接：{addr}")
            self.clients[conn] = addr
            # 为每个客户端启动独立的消息处理线程
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

    def handle_client(self, conn, addr):
        """处理单个客户端的消息（无心跳检测，依赖 recv 阻塞）"""
        try:
            while True:
                # 阻塞接收客户端数据（无超时设置）
                data = conn.recv(1024)  # 若客户端断开，recv 返回空字节 b''
                if not data:
                    # 客户端主动断开或网络异常导致连接失效
                    print(f"[{datetime.now().isoformat()}] 客户端 {addr} 断开连接")
                    break
                # 打印并回复客户端消息（模拟业务逻辑）
                print(f"[{datetime.now().isoformat()}] 收到 {addr} 消息：{data.decode('utf-8')}")
                conn.sendall(f"服务端已接收：{data.decode('utf-8')}".encode('utf-8'))
        except ConnectionResetError:
            # 客户端强制断开（如 kill 进程）时的异常
            print(f"[{datetime.now().isoformat()}] 客户端 {addr} 异常断开（连接被重置）")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] 处理 {addr} 错误：{str(e)}")
        finally:
            # 清理连接资源
            conn.close()
            if conn in self.clients:
                del self.clients[conn]

if __name__ == "__main__":
    server = TCPServer()
    server.start()
