import socket
import threading
import time
from datetime import datetime

class ConcurrentTCPClient:
    def __init__(self, target_host='192.168.100.10', target_port=8888, 
                 local_port_start=40000, count=20, query_interval=60):
        """
        初始化长连接客户端（含3分钟业务查询）
        :param target_host: 目标服务器IP
        :param target_port: 目标服务器端口
        :param local_port_start: 本地源端口起始值（生成count个连续端口）
        :param count: 需要发起的长连接数量
        :param query_interval: 业务查询间隔（秒，默认180秒=3分钟）
        """
        self.target_host = target_host
        self.target_port = target_port
        self.local_port_start = local_port_start
        self.count = count  # 总连接数（20）
        self.query_interval = query_interval  # 业务查询间隔（3分钟）
        self.connections = []  # 保存所有活跃的连接对象

    def _get_local_ports(self):
        """生成本地源端口列表（30000-30019，共20个）"""
        return [self.local_port_start + i for i in range(self.count)]

    def _connect_and_listen(self, local_port):
        """单个连接的建立、消息接收（核心逻辑）"""
        try:
            # 创建TCP Socket并绑定本地端口
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 允许端口复用
            sock.bind(('0.0.0.0', local_port))  # 绑定到指定端口
            
            # 连接目标服务器
            sock.connect((self.target_host, self.target_port))
            print(f"[{datetime.now().isoformat()}] 连接成功 | 本地端口: {local_port} | 目标: {self.target_host}:{self.target_port}")
            
            # 记录连接
            self.connections.append(sock)
            
            # 启动消息接收线程（持续监听服务端消息）
            threading.Thread(target=self._receive_messages, args=(sock,), daemon=True).start()
            
            # 启动业务查询发送线程（核心新增逻辑）
            threading.Thread(target=self._send_business_queries, args=(sock,), daemon=True).start()
            
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] 连接失败 | 本地端口: {local_port} | 错误: {str(e)}")

    def _receive_messages(self, sock):
        """接收服务端消息（无超时，依赖recv阻塞）"""
        local_port = sock.getsockname()[1]
        try:
            while True:
                data = sock.recv(1024)  # 阻塞接收（无超时设置）
                if not data:
                    # 服务端断开连接（客户端无主动感知，仅通过recv返回空判断）
                    print(f"[{datetime.now().isoformat()}] 连接断开 | 本地端口: {local_port}（服务端关闭）")
                    sock.close()
                    self.connections.remove(sock)
                    break
                print(f"[{datetime.now().isoformat()}] 收到消息 | 本地端口: {local_port} | 内容: {data.decode('utf-8')}")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] 接收异常 | 本地端口: {local_port} | 错误: {str(e)}")
            sock.close()
            if sock in self.connections:
                self.connections.remove(sock)

    def _send_business_queries(self, sock):
        """定时发送业务查询报文（核心新增逻辑）"""
        local_port = sock.getsockname()[1]
        last_query_time = time.time()  # 记录上次查询时间
        
        try:
            while True:
                current_time = time.time()
                # 检查是否达到查询间隔（避免首次等待过久）
                if current_time - last_query_time >= self.query_interval:
                    # 构造业务查询报文（示例：包含时间戳和连接标识）
                    query_msg = f"QUERY|{local_port}|{datetime.now().isoformat()}"
                    # 发送查询报文
                    sock.sendall(query_msg.encode('utf-8'))
                    print(f"[{datetime.now().isoformat()}] 发送业务查询 | 本地端口: {local_port} | 内容: {query_msg}")
                    last_query_time = current_time  # 更新上次查询时间
                
                # 短睡眠避免CPU空转（每1秒检查一次）
                time.sleep(1)
                
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] 业务查询发送异常 | 本地端口: {local_port} | 错误: {str(e)}")
            sock.close()
            if sock in self.connections:
                self.connections.remove(sock)

    def start_all_connections(self):
        """启动所有长连接的核心方法"""
        local_ports = self._get_local_ports()
        print(f"[{datetime.now().isoformat()}] 准备启动 {self.count} 个长连接 | 本地端口范围: {local_ports}")
        
        # 为每个端口创建独立线程发起连接
        threads = []
        for port in local_ports:
            thread = threading.Thread(target=self._connect_and_listen, args=(port,), daemon=True)
            threads.append(thread)
            thread.start()  # 立即启动线程
        
        # 主线程保持运行（防止程序退出）
        try:
            while True:
                time.sleep(60)
                # 可选：定期打印存活连接数
                print(f"[{datetime.now().isoformat()}] 当前存活连接数: {len(self.connections)}")
        except KeyboardInterrupt:
            print("\n用户终止程序，正在关闭所有连接...")

if __name__ == "__main__":
    # 配置参数（可根据需求调整）
    client = ConcurrentTCPClient(
        target_host='192.168.100.10',  # 目标服务器IP
        target_port=8888,              # 目标服务器端口
        local_port_start=40000,        # 本地源端口起始值
        count=20                       # 总连接数（20）
    )
    
    # 启动所有长连接
    client.start_all_connections()
