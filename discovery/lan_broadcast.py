"""
discovery/lan_broadcast.py - HYBRID VERSION
1. Quick app-level discovery (devices with our service)
2. Optional network scan (all devices - for awareness)
"""

import socket
import json
import threading
import time
import uuid
import platform
import subprocess
import ipaddress
from typing import Callable, Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

class DeviceType(Enum):
    OUR_APP = "our_app"          # Running our service
    COMPUTER = "computer"        # PC/Mac without our app
    PHONE = "phone"              # Mobile device
    UNKNOWN = "unknown"          # Can't identify

@dataclass
class DeviceInfo:
    """Information about a discovered device"""
    device_id: str
    device_name: str
    ip_address: str
    port: int
    platform: str
    device_type: DeviceType
    has_our_app: bool
    last_seen: float
    is_online: bool = True
    mac_address: Optional[str] = None
    vendor: Optional[str] = None  # Apple, Samsung, etc.
    
    def to_dict(self):
        return asdict(self)
        
    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

class HybridLANDiscovery:
    """
    Hybrid LAN discovery - detects both:
    1. Devices running our app (via UDP broadcast)
    2. All network devices (via ARP scan)
    """
    
    BROADCAST_PORT = 9999
    TCP_PORT = 8888
    BROADCAST_INTERVAL = 5
    DEVICE_TIMEOUT = 15
    NETWORK_SCAN_INTERVAL = 30  # Scan network every 30 seconds
    
    def __init__(self, device_name: str = None, 
                 on_device_found: Callable = None,
                 on_device_lost: Callable = None,
                 enable_network_scan: bool = True):
        """
        Initialize hybrid discovery
        
        Args:
            device_name: Name of this device
            on_device_found: Callback when new device discovered
            on_device_lost: Callback when device goes offline
            enable_network_scan: Also scan for all network devices
        """
        self.device_id = str(uuid.uuid4())
        self.device_name = device_name or platform.node()
        self.platform = platform.system()
        self.enable_network_scan = enable_network_scan
        
        self.on_device_found = on_device_found
        self.on_device_lost = on_device_lost
        
        # All discovered devices (both types)
        self.devices: Dict[str, DeviceInfo] = {}
        self.lock = threading.Lock()
        
        # Network info
        self.local_ip = self._get_local_ip()
        self.network_range = self._get_network_range()
        
        # Sockets
        self.broadcast_socket = None
        self.listen_socket = None
        
        # Threads
        self.is_running = False
        self.threads = []
        
        print(f"[HybridDiscovery] Local IP: {self.local_ip}")
        print(f"[HybridDiscovery] Network: {self.network_range}")
        
    def start(self):
        """Start discovery service"""
        if self.is_running:
            return
            
        self.is_running = True
        
        # Setup app-level discovery
        self._setup_sockets()
        
        # Start app discovery threads
        self._start_thread(self._broadcast_loop, "Broadcast")
        self._start_thread(self._listen_loop, "Listener")
        self._start_thread(self._cleanup_loop, "Cleanup")
        
        # Start network scanner (for all devices)
        if self.enable_network_scan:
            self._start_thread(self._network_scan_loop, "NetworkScanner")
            
        print(f"[HybridDiscovery] Started - Device: {self.device_name}")
        
    def _start_thread(self, target, name):
        """Start a daemon thread"""
        thread = threading.Thread(target=target, daemon=True, name=name)
        thread.start()
        self.threads.append(thread)
        
    def _get_network_range(self) -> str:
        """Get local network range (e.g., 192.168.1.0/24)"""
        try:
            ip = ipaddress.ip_address(self.local_ip)
            network = ipaddress.ip_network(f"{ip}/24", strict=False)
            return str(network)
        except:
            return "192.168.1.0/24"
            
    def _network_scan_loop(self):
        """Periodically scan network for all devices"""
        while self.is_running:
            try:
                print("[HybridDiscovery] 🔍 Scanning network for all devices...")
                devices = self._scan_network()
                
                for device in devices:
                    self._add_or_update_network_device(device)
                    
                print(f"[HybridDiscovery] Found {len(devices)} network devices")
                
            except Exception as e:
                print(f"[HybridDiscovery] Scan error: {e}")
                
            time.sleep(self.NETWORK_SCAN_INTERVAL)
            
    def _scan_network(self) -> List[DeviceInfo]:
        """Scan network using ARP and ping"""
        devices = []
        
        # Method 1: ARP table (shows recently active devices)
        arp_devices = self._get_arp_table()
        devices.extend(arp_devices)
        
        # Method 2: Quick ping sweep (optional - can be slow)
        # Only do this if ARP table is empty
        if not devices:
            ping_devices = self._ping_sweep()
            devices.extend(ping_devices)
            
        return devices
        
    def _get_arp_table(self) -> List[DeviceInfo]:
        """Get ARP table entries"""
        devices = []
        
        try:
            if self.platform == "Windows":
                output = subprocess.check_output("arp -a", shell=True).decode()
                devices = self._parse_windows_arp(output)
            elif self.platform == "Darwin":  # macOS
                output = subprocess.check_output("arp -a", shell=True).decode()
                devices = self._parse_unix_arp(output)
            else:  # Linux
                output = subprocess.check_output("arp -n", shell=True).decode()
                devices = self._parse_unix_arp(output)
        except Exception as e:
            print(f"[HybridDiscovery] ARP scan failed: {e}")
            
        return devices
        
    def _parse_windows_arp(self, output: str) -> List[DeviceInfo]:
        """Parse Windows ARP output"""
        devices = []
        lines = output.split('\n')
        
        for line in lines:
            parts = line.split()
            if len(parts) >= 3:
                ip = parts[0]
                mac = parts[1]
                if self._is_valid_ip(ip) and mac != "ff-ff-ff-ff-ff-ff":
                    # Skip self
                    if ip == self.local_ip:
                        continue
                        
                    device_type = self._guess_device_type(mac, ip)
                    vendor = self._get_vendor_from_mac(mac)
                    
                    device = DeviceInfo(
                        device_id=f"net-{mac.replace('-', '')}",
                        device_name=vendor or ip,
                        ip_address=ip,
                        port=0,
                        platform="Unknown",
                        device_type=device_type,
                        has_our_app=False,
                        last_seen=time.time(),
                        is_online=True,
                        mac_address=mac,
                        vendor=vendor
                    )
                    devices.append(device)
                    
        return devices
        
    def _parse_unix_arp(self, output: str) -> List[DeviceInfo]:
        """Parse Unix/Linux/macOS ARP output"""
        devices = []
        lines = output.split('\n')
        
        for line in lines:
            # Format: "192.168.1.15 (192.168.1.15) at aa:bb:cc:dd:ee:ff [ether] on eth0"
            import re
            ip_match = re.search(r'\(?(\d+\.\d+\.\d+\.\d+)\)?', line)
            mac_match = re.search(r'([0-9a-f]{1,2}[:-]){5}[0-9a-f]{1,2}', line, re.I)
            
            if ip_match and mac_match:
                ip = ip_match.group(1)
                mac = mac_match.group(0)
                
                if self._is_valid_ip(ip) and ip != self.local_ip:
                    vendor = self._get_vendor_from_mac(mac)
                    device_type = self._guess_device_type(mac, ip)
                    
                    device = DeviceInfo(
                        device_id=f"net-{mac.replace(':', '').replace('-', '')}",
                        device_name=vendor or ip,
                        ip_address=ip,
                        port=0,
                        platform="Unknown",
                        device_type=device_type,
                        has_our_app=False,
                        last_seen=time.time(),
                        is_online=True,
                        mac_address=mac,
                        vendor=vendor
                    )
                    devices.append(device)
                    
        return devices
        
    def _ping_sweep(self) -> List[DeviceInfo]:
        """Quick ping sweep of local network (only if ARP fails)"""
        devices = []
        
        try:
            network = ipaddress.ip_network(self.network_range)
            
            # Only scan first 20 addresses (quick scan)
            for ip in list(network.hosts())[:20]:
                ip_str = str(ip)
                if ip_str == self.local_ip:
                    continue
                    
                # Single ping with 100ms timeout
                if self._ping_host(ip_str):
                    device = DeviceInfo(
                        device_id=f"net-{ip_str.replace('.', '')}",
                        device_name=ip_str,
                        ip_address=ip_str,
                        port=0,
                        platform="Unknown",
                        device_type=DeviceType.UNKNOWN,
                        has_our_app=False,
                        last_seen=time.time(),
                        is_online=True
                    )
                    devices.append(device)
                    
        except Exception as e:
            print(f"[HybridDiscovery] Ping sweep failed: {e}")
            
        return devices
        
    def _ping_host(self, ip: str) -> bool:
        """Ping a single host"""
        try:
            if self.platform == "Windows":
                result = subprocess.run(
                    ["ping", "-n", "1", "-w", "100", ip],
                    capture_output=True, timeout=1
                )
            else:
                result = subprocess.run(
                    ["ping", "-c", "1", "-W", "1", ip],
                    capture_output=True, timeout=1
                )
            return result.returncode == 0
        except:
            return False
            
    def _is_valid_ip(self, ip: str) -> bool:
        """Check if string is valid IP"""
        try:
            ipaddress.ip_address(ip)
            return True
        except:
            return False
            
    def _guess_device_type(self, mac: str, ip: str) -> DeviceType:
        """Guess device type from MAC address or hostname"""
        mac_upper = mac.upper()
        
        # Common OUIs (Organizationally Unique Identifier)
        apple_ouis = ['00:03:93', '00:0A:27', '00:0A:95', '00:0D:93', 
                      '00:11:24', '00:14:51', '00:16:CB', '00:17:F2']
        samsung_ouis = ['00:16:6B', '00:1E:DF', '00:23:D4', '00:26:E8']
        android_ouis = ['00:90:4C', '08:00:28', '0C:1D:AF']
        
        mac_prefix = mac[:8]
        
        if any(mac.startswith(oui) for oui in apple_ouis):
            return DeviceType.PHONE  # Could be iPhone/Mac
        elif any(mac.startswith(oui) for oui in samsung_ouis + android_ouis):
            return DeviceType.PHONE
        else:
            return DeviceType.COMPUTER
            
    def _get_vendor_from_mac(self, mac: str) -> Optional[str]:
        """Get vendor name from MAC address OUI"""
        # Simplified - in production, use a proper OUI database
        mac_upper = mac.upper()
        
        vendors = {
            '00:03:93': 'Apple',
            '00:0A:27': 'Apple',
            '00:11:24': 'Apple',
            '00:16:CB': 'Apple',
            '00:16:6B': 'Samsung',
            '00:1E:DF': 'Samsung',
            '00:23:D4': 'Samsung',
            '08:00:28': 'Android',
            '3C:5A:B4': 'Google',
            'B8:27:EB': 'Raspberry Pi',
            'DC:A6:32': 'Raspberry Pi',
        }
        
        for prefix, vendor in vendors.items():
            if mac.startswith(prefix):
                return vendor
                
        return None
        
    def _add_or_update_network_device(self, device: DeviceInfo):
        """Add or update network device in list"""
        with self.lock:
            device_id = device.device_id
            
            is_new = device_id not in self.devices
            
            # Don't overwrite app devices with network scan
            if device_id in self.devices:
                existing = self.devices[device_id]
                if existing.has_our_app:
                    return  # Keep app device info
                    
            device.last_seen = time.time()
            self.devices[device_id] = device
            
            if is_new and self.on_device_found:
                self.on_device_found(device)
                
    def _setup_sockets(self):
        """Setup UDP sockets for app-level discovery"""
        # ... (same as before) ...
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind(('', self.BROADCAST_PORT))
        self.listen_socket.settimeout(1.0)
        
    def _broadcast_loop(self):
        """Broadcast presence to other app instances"""
        while self.is_running:
            try:
                message = {
                    'type': 'announce',
                    'device_id': self.device_id,
                    'device_name': self.device_name,
                    'ip_address': self.local_ip,
                    'port': self.TCP_PORT,
                    'platform': self.platform,
                    'timestamp': time.time()
                }
                
                data = json.dumps(message).encode('utf-8')
                self.broadcast_socket.sendto(data, ('255.255.255.255', self.BROADCAST_PORT))
                
            except Exception as e:
                print(f"[HybridDiscovery] Broadcast error: {e}")
                
            time.sleep(self.BROADCAST_INTERVAL)
            
    def _listen_loop(self):
        """Listen for app-level announcements"""
        while self.is_running:
            try:
                data, addr = self.listen_socket.recvfrom(4096)
                message = json.loads(data.decode('utf-8'))
                
                if message.get('device_id') == self.device_id:
                    continue
                    
                if message.get('type') == 'announce':
                    device = DeviceInfo(
                        device_id=message['device_id'],
                        device_name=message['device_name'],
                        ip_address=message['ip_address'],
                        port=message['port'],
                        platform=message['platform'],
                        device_type=DeviceType.OUR_APP,
                        has_our_app=True,
                        last_seen=time.time(),
                        is_online=True
                    )
                    
                    with self.lock:
                        is_new = device.device_id not in self.devices
                        self.devices[device.device_id] = device
                        
                    if is_new and self.on_device_found:
                        self.on_device_found(device)
                        
            except socket.timeout:
                continue
            except Exception as e:
                if self.is_running:
                    print(f"[HybridDiscovery] Listen error: {e}")
                    
    def _cleanup_loop(self):
        """Remove offline devices"""
        while self.is_running:
            time.sleep(2)
            
            current_time = time.time()
            offline_devices = []
            
            with self.lock:
                for device_id, device in list(self.devices.items()):
                    timeout = self.DEVICE_TIMEOUT
                    if not device.has_our_app:
                        timeout = 60  # Network devices timeout longer
                        
                    if current_time - device.last_seen > timeout:
                        device.is_online = False
                        offline_devices.append(device)
                        del self.devices[device_id]
                        
            for device in offline_devices:
                if self.on_device_lost:
                    self.on_device_lost(device)
                    
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return '127.0.0.1'
            
    def stop(self):
        """Stop discovery service"""
        self.is_running = False
        
        if self.broadcast_socket:
            self.broadcast_socket.close()
        if self.listen_socket:
            self.listen_socket.close()
            
        print("[HybridDiscovery] Stopped")
        
    def get_online_devices(self, app_only: bool = True) -> List[DeviceInfo]:
        """Get online devices"""
        with self.lock:
            if app_only:
                return [d for d in self.devices.values() 
                       if d.is_online and d.has_our_app]
            else:
                return [d for d in self.devices.values() if d.is_online]
                
    def get_devices_with_app(self) -> List[DeviceInfo]:
        """Get only devices running our app"""
        return self.get_online_devices(app_only=True)


# ============================================
# TEST FUNCTION
# ============================================

def test_hybrid_discovery():
    """Test hybrid discovery"""
    
    def on_device_found(device: DeviceInfo):
        icon = "🟢" if device.has_our_app else "🔵"
        print(f"\n{icon} DEVICE FOUND: {device.device_name}")
        print(f"   IP: {device.ip_address}")
        print(f"   Type: {device.device_type.value}")
        print(f"   Has App: {device.has_our_app}")
        if device.vendor:
            print(f"   Vendor: {device.vendor}")
            
    discovery = HybridLANDiscovery(
        device_name="Test-Device",
        on_device_found=on_device_found,
        enable_network_scan=True  # Scan all devices
    )
    
    discovery.start()
    
    print("\n🔍 Hybrid Discovery Active")
    print("   - Detecting devices with our app (UDP)")
    print("   - Scanning network for all devices (ARP)")
    print("\n   Press Enter to see all devices")
    print("   Press Ctrl+C to exit\n")
    
    try:
        while True:
            cmd = input()
            if cmd == "":
                app_devices = discovery.get_devices_with_app()
                all_devices = discovery.get_online_devices(app_only=False)
                
                print(f"\n📱 Devices with our app ({len(app_devices)}):")
                for d in app_devices:
                    print(f"   • {d.device_name} ({d.ip_address}) - READY FOR TRANSFER")
                    
                print(f"\n🌐 All network devices ({len(all_devices)}):")
                for d in all_devices[:10]:  # Show first 10
                    status = "✅" if d.has_our_app else "⭕"
                    print(f"   {status} {d.device_name} ({d.ip_address})")
                    
    except KeyboardInterrupt:
        print("\nStopping...")
        discovery.stop()


if __name__ == "__main__":
    test_hybrid_discovery()