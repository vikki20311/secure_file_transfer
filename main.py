"""
main.py
Central Controller - Secure File Transfer System
Orchestrates all modules for gesture-based file transfer
"""

import os
import sys
import time
import threading
import uuid
import json
from typing import Optional, Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import all modules
from gesture.hand_detector import HandDetector, GestureType
from ui.overlay import OverlayUI, UIState, UIData, TransferMode
from context.content_detector import ContentDetector, ContentType, ContentInfo
from discovery.lan_broadcast import HybridLANDiscovery, DeviceInfo, DeviceType
from discovery.server_lookup import ServerLookup, RegisteredUser, UserStatus
from security.hybrid import HybridTransferHandler
from transfer.chunker import FileChunker, FileAssembler, FileMetadata
from transfer.parallel_upload import ParallelUploader, UploadMode, UploadProgress
from receiver.listener import ReceiverListener, IncomingTransfer, TransferType
from receiver.acceptor import TransferAcceptor, AcceptanceResult, AcceptanceMethod
from receiver.download_manager import DownloadManager, DownloadMode, DownloadProgress
from reliability.ack_manager import ACKManager
from reliability.retry_queue import RetryQueue, RetryStrategy
from reliability.timeout_manager import TimeoutManager, TimeoutType, TransferState
from integrity.sha256_verify import IntegrityVerifier
from connection.tcp_direct import TCPConnectionManager, TCPConnection
from connection.relay_client import RelayClient, RelayConfig


class SecureTransferController:
    """
    Main controller - Orchestrates the entire system
    Manages the global session object and coordinates all modules
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the secure transfer controller
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        
        # ============================================
        # GLOBAL SESSION OBJECT
        # ============================================
        self.session = {
            # Device Info
            'device_id': str(uuid.uuid4()),
            'device_name': self.config.get('device_name') or 'SecureTransfer',
            'local_ip': self._get_local_ip(),
            
            # Current Context
            'active_content': None,
            'active_file': None,
            'content_type': None,
            'file_size': 0,
            'file_hash': None,
            
            # Transfer State
            'mode': None,  # 'lan' or 'wan'
            'target_device_id': None,
            'target_device_ip': None,
            'target_user_id': None,
            'transfer_id': None,
            'is_sender': False,
            'is_receiver': False,
            
            # Security
            'private_key': None,
            'public_key': None,
            'shared_secret': None,
            'aes_key': None,
            'hybrid_handler': None,
            
            # Chunk Management
            'total_chunks': 0,
            'chunks': [],
            'encrypted_chunks': [],
            'metadata': None,
            'encrypted_metadata': None,
            
            # Connection
            'tcp_connection': None,
            'relay_client': None,
            'uploader': None,
            'downloader': None,
            
            # Threading
            'camera_thread': None,
            'transfer_threads': [],
            
            # UI
            'overlay': None,
        }
        
        # ============================================
        # INITIALIZE MODULES
        # ============================================
        
        # Simplify device name setting
        import socket as sock_module
        try:
            hostname = sock_module.gethostname()
        except:
            hostname = 'SecureTransfer'
        
        if not self.session['device_name'] or self.session['device_name'] == 'SecureTransfer':
            self.session['device_name'] = self.config.get('device_name') or hostname
        
        # Gesture Detection
        self.gesture_detector = HandDetector(callback=self._on_gesture_detected)
        
        # UI Overlay - Only initialize if needed
        self.overlay = None
        self._init_ui_later = True
        
        # Content Detector
        self.content_detector = ContentDetector(
            on_content_changed=self._on_content_changed
        )
        
        # LAN Discovery
        self.lan_discovery = HybridLANDiscovery(
            device_name=self.session['device_name'],
            on_device_found=self._on_device_found,
            on_device_lost=self._on_device_lost,
            enable_network_scan=True
        )
        
        # Server Lookup (WAN) - Only if server URL is valid
        server_url = self.config.get('server_url', '')
        self.server_lookup = None
        self.use_wan = False
        
        if server_url and server_url.startswith('http'):
            self.server_lookup = ServerLookup(
                server_url=server_url,
                on_user_status_change=self._on_user_status_change
            )
            self.use_wan = True
        
        # Receiver Listener (LAN always active)
        self.receiver_listener = ReceiverListener(
            server_url=server_url if self.use_wan else None
        )
        
        # Transfer Acceptor
        self.transfer_acceptor = TransferAcceptor(
            listener=self.receiver_listener,
            gesture_detector=self.gesture_detector,
            ui_overlay=None  # Will set later
        )
        self.transfer_acceptor.on_accepted = self._on_transfer_accepted
        self.transfer_acceptor.on_rejected = self._on_transfer_rejected
        
        # Timeout Manager
        self.timeout_manager = TimeoutManager()
        self.timeout_manager.on_timeout_expired = self._on_timeout_expired
        
        # TCP Connection Manager
        self.tcp_manager = TCPConnectionManager()
        
        # State
        self.is_running = False
        self.current_state = UIState.HIDDEN
        self.selected_mode = None
        
        print(f"[Controller] ✅ Initialized")
        print(f"             Device: {self.session['device_name']}")
        print(f"             Local IP: {self.session['local_ip']}")
        print(f"             Mode: {'LAN + WAN' if self.use_wan else 'LAN Only'}")
        
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return '127.0.0.1'
            
    def start(self):
        """Start the secure transfer system"""
        if self.is_running:
            return
            
        self.is_running = True
        
        # Initialize UI (lazy init to avoid early errors)
        if self._init_ui_later:
            try:
                self.overlay = OverlayUI(
                    on_mode_selected=self._on_mode_selected,
                    on_recipient_selected=self._on_recipient_selected,
                    on_cancel=self._on_cancel
                )
                self.session['overlay'] = self.overlay
                self.transfer_acceptor.ui_overlay = self.overlay
                self._init_ui_later = False
            except Exception as e:
                print(f"[Controller] UI init skipped (headless mode): {e}")
                self.overlay = None
        
        # Start gesture detection
        self.gesture_detector.start()
        
        # Start content detection
        self.content_detector.start_monitoring()
        
        # Start LAN discovery
        self.lan_discovery.start()
        
        # Start receiver listener (LAN only for now, WAN only if configured)
        self.receiver_listener.lan_listener.start()
        self.receiver_listener.on_transfer_request = self._on_incoming_transfer
        
        if self.use_wan and self.receiver_listener.wan_listener:
            try:
                self.receiver_listener.wan_listener.start()
            except Exception as e:
                print(f"[Controller] WAN listener skipped: {e}")
        
        self.receiver_listener.is_running = True
        
        # Start timeout manager
        self.timeout_manager.start()
        
        print("[Controller] ✅ System started")
        print("             Waiting for gestures...")
        print("             🤜 CLOSE FIST to activate")
        
    def stop(self):
        """Stop the secure transfer system"""
        self.is_running = False
        
        self.gesture_detector.stop()
        self.content_detector.stop_monitoring()
        self.lan_discovery.stop()
        self.receiver_listener.stop()
        self.timeout_manager.stop()
        
        # Clean up session
        if self.session.get('hybrid_handler'):
            self.session['hybrid_handler'].cleanup()
            
        print("[Controller] System stopped")
        
    # ============================================
    # GESTURE HANDLERS
    # ============================================
    
    def _on_gesture_detected(self, gesture_type: GestureType, pointer_pos, frame):
        """Handle detected gestures"""
        
        if gesture_type == GestureType.CLOSE_FIST:
            self._handle_close_fist()
        elif gesture_type == GestureType.OPEN_PALM:
            self._handle_open_palm()
        elif gesture_type == GestureType.POINTING:
            self._handle_pointing(pointer_pos)
            
    def _handle_close_fist(self):
        """Handle close fist gesture - Activate system"""
        if self.current_state == UIState.HIDDEN:
            # Activate - show mode selection
            content = self.content_detector.get_current_content()
            
            if content:
                self.session['active_content'] = content
                self.session['active_file'] = content.source_path
                self.session['content_type'] = content.content_type
                self.session['file_size'] = content.size_bytes
                
                self.current_state = UIState.ACTIVATED
                
                print(f"\n[Controller] 🤜 ACTIVATED!")
                print(f"             Content: {content.display_name}")
                print(f"             Type: {content.content_type.value}")
                
                # Show UI if available
                if self.overlay:
                    try:
                        ui_data = UIData(
                            state=UIState.ACTIVATED,
                            current_file=content.display_name,
                            file_size=content.size_formatted
                        )
                        self.overlay.show(ui_data)
                    except Exception as e:
                        print(f"             UI: {e}")
                
                # Auto-select mode based on available recipients
                print(f"             👆 POINT LEFT for LAN | RIGHT for WAN")
                
        elif self.current_state == UIState.READY_TO_SEND:
            # Cancel sending
            if self.overlay:
                try:
                    self.overlay.hide()
                except:
                    pass
            self.current_state = UIState.HIDDEN
            print("[Controller] Cancelled")
            
    def _handle_open_palm(self):
        """Handle open palm gesture - Send file"""
        if self.current_state == UIState.READY_TO_SEND:
            print(f"\n[Controller] 🖐️ SENDING FILE...")
            self._start_transfer()
        elif self.current_state == UIState.ACTIVATED:
            print(f"\n[Controller] Select recipient first (POINT gesture)")
            
    def _handle_pointing(self, pointer_pos):
        """Handle pointing gesture - Selection"""
        if self.current_state == UIState.ACTIVATED and pointer_pos:
            x = pointer_pos[0]
            
            if x < 0.5:
                self.selected_mode = TransferMode.LAN
                print(f"[Controller] 📡 Selected: LAN")
            else:
                self.selected_mode = TransferMode.WAN
                print(f"[Controller] 🌐 Selected: WAN")
            
            # Get recipients
            recipients = self._get_available_recipients()
            
            if recipients:
                self.current_state = UIState.READY_TO_SEND
                print(f"[Controller] 🎯 Recipients available: {len(recipients)}")
                for r in recipients:
                    print(f"             • {r}")
                print(f"             🖐️ OPEN PALM to send to first available")
                
                # Auto-select first recipient
                if recipients:
                    self.session['target_device_name'] = recipients[0]
            else:
                print(f"[Controller] ⚠️ No recipients available")
                if self.selected_mode == TransferMode.LAN:
                    print(f"             Make sure another device is on same WiFi")
                else:
                    print(f"             Make sure relay server is running")
            
            # Update UI
            if self.overlay:
                try:
                    if self.selected_mode == TransferMode.LAN:
                        devices = self.lan_discovery.get_devices_with_app()
                        recipients = [d.device_name for d in devices]
                        self.overlay.update_recipients(recipients)
                    else:
                        if self.server_lookup:
                            users = self.server_lookup.get_online_users()
                            recipients = [u.display_name for u in users]
                            self.overlay.update_recipients(recipients)
                    self.overlay.set_state(UIState.MODE_SELECTED)
                except Exception as e:
                    pass
            
    def _get_available_recipients(self) -> list:
        """Get list of available recipients"""
        recipients = []
        
        if self.selected_mode == TransferMode.LAN:
            devices = self.lan_discovery.get_devices_with_app()
            recipients = [d.device_name for d in devices]
        else:
            if self.server_lookup:
                users = self.server_lookup.get_online_users()
                recipients = [u.display_name for u in users]
                
        return recipients
        
    # ============================================
    # UI CALLBACKS
    # ============================================
    
    def _on_mode_selected(self, mode: TransferMode):
        """Handle mode selection (LAN/WAN)"""
        self.selected_mode = mode
        
        recipients = self._get_available_recipients()
        
        if self.overlay:
            try:
                self.overlay.update_recipients(recipients)
                self.current_state = UIState.MODE_SELECTED
            except:
                pass
            
        print(f"[Controller] Mode selected: {mode.value}")
        
    def _on_recipient_selected(self, recipient: str):
        """Handle recipient selection"""
        self.session['target_device_name'] = recipient
        self.current_state = UIState.READY_TO_SEND
        
        print(f"[Controller] Recipient selected: {recipient}")
        print(f"             🖐️ OPEN PALM to send")
        
    def _on_cancel(self):
        """Handle cancel action"""
        self.current_state = UIState.HIDDEN
        self.selected_mode = None
        print("[Controller] Cancelled")
        
    # ============================================
    # CONTENT DETECTION
    # ============================================
    
    def _on_content_changed(self, content: ContentInfo):
        """Handle content change"""
        self.session['active_content'] = content
        # Only print if system is active
        if self.current_state != UIState.HIDDEN:
            print(f"[Controller] Content: {content.display_name}")
        
    # ============================================
    # DEVICE DISCOVERY
    # ============================================
    
    def _on_device_found(self, device: DeviceInfo):
        """Handle discovered LAN device"""
        if device.has_our_app:
            print(f"[Controller] 🟢 Device found: {device.device_name} ({device.ip_address})")
        
    def _on_device_lost(self, device: DeviceInfo):
        """Handle lost LAN device"""
        if device.has_our_app:
            print(f"[Controller] 🔴 Device lost: {device.device_name}")
        
    def _on_user_status_change(self, user: RegisteredUser, is_online: bool):
        """Handle user online/offline status"""
        status = "online" if is_online else "offline"
        print(f"[Controller] User {user.display_name} is now {status}")
        
    # ============================================
    # INCOMING TRANSFERS
    # ============================================
    
    def _on_incoming_transfer(self, transfer: IncomingTransfer):
        """Handle incoming transfer request"""
        print(f"\n[Controller] 📨 INCOMING TRANSFER!")
        print(f"             From: {transfer.sender_name}")
        print(f"             File: {transfer.filename}")
        print(f"             Size: {transfer.format_size()}")
        print(f"             🤜 CLOSE FIST to accept")
        
        # Store in session
        self.session['incoming_transfer'] = transfer
        self.session['is_receiver'] = True
        self.session['transfer_id'] = transfer.transfer_id
        
        # Register with timeout manager
        timeout_type = TimeoutType.LAN if transfer.transfer_type == TransferType.LAN else TimeoutType.WAN
        self.timeout_manager.register_transfer(
            transfer.transfer_id,
            timeout_type,
            metadata={'filename': transfer.filename, 'sender': transfer.sender_name}
        )
        
        # Show UI if available
        if self.overlay:
            try:
                ui_data = UIData(
                    state=UIState.INCOMING_REQUEST,
                    current_file=transfer.filename,
                    file_size=transfer.format_size(),
                    sender_name=transfer.sender_name,
                    message=f"{transfer.sender_name} wants to send:\n{transfer.filename}"
                )
                self.overlay.show(ui_data)
            except:
                pass
        
    def _on_transfer_accepted(self, transfer: IncomingTransfer, result: AcceptanceResult):
        """Handle accepted transfer"""
        print(f"[Controller] ✅ Transfer accepted via {result.method.value}")
        
        # Initialize security
        self.session['hybrid_handler'] = HybridTransferHandler()
        self.session['hybrid_handler'].init_as_receiver(b"")  # Will get sender's public key
        
        # Start download
        self._start_download(transfer)
        
    def _on_transfer_rejected(self, transfer: IncomingTransfer, result: AcceptanceResult):
        """Handle rejected transfer"""
        print(f"[Controller] ❌ Transfer rejected")
        self.session['is_receiver'] = False
        
    def _on_timeout_expired(self, transfer_id: str, info):
        """Handle transfer timeout"""
        print(f"[Controller] ⏰ Transfer {transfer_id[:8]}... expired")
        
    # ============================================
    # TRANSFER LOGIC
    # ============================================
    
    def _start_transfer(self):
        """Start file transfer"""
        content = self.session.get('active_content')
        
        if not content:
            print("[Controller] No content to send")
            return
            
        self.session['is_sender'] = True
        transfer_id = str(uuid.uuid4())
        self.session['transfer_id'] = transfer_id
        
        print(f"[Controller] 📤 Preparing to send: {content.display_name}")
        
        # Update UI
        if self.overlay:
            try:
                self.overlay.set_state(UIState.TRANSFERRING, "Preparing file...")
            except:
                pass
        
        # Initialize security
        self.session['hybrid_handler'] = HybridTransferHandler()
        sender_public = self.session['hybrid_handler'].init_as_sender()
        print(f"[Controller] 🔐 Encryption initialized")
        
        # Prepare file based on content type
        if content.content_type == ContentType.SCREENSHOT:
            screenshot_data = self.content_detector.capture_screenshot()
            if screenshot_data:
                print(f"[Controller] 📸 Screenshot captured: {len(screenshot_data)} bytes")
                if self.overlay:
                    try:
                        self.overlay.set_state(UIState.COMPLETE, "Screenshot sent!")
                    except:
                        pass
            else:
                print(f"[Controller] ❌ Screenshot failed")
        else:
            if content.can_send_file and content.source_path:
                self._send_file(content.source_path, transfer_id)
            elif content.can_send_link and content.source_path:
                self._send_link(content.source_path, transfer_id)
            else:
                # Default to screenshot
                screenshot_data = self.content_detector.capture_screenshot()
                if screenshot_data:
                    print(f"[Controller] 📸 Fallback screenshot: {len(screenshot_data)} bytes")
                
    def _send_file(self, file_path: str, transfer_id: str):
        """Send a file"""
        print(f"[Controller] 📁 Processing file: {file_path}")
        
        try:
            # Chunk file
            chunker = FileChunker()
            metadata = chunker.split_file(file_path)
            
            self.session['metadata'] = metadata
            self.session['total_chunks'] = metadata.total_chunks
            self.session['chunks'] = chunker.get_all_chunks()
            
            print(f"[Controller] 📦 Split into {metadata.total_chunks} chunks")
            
            # Encrypt file
            handler = self.session['hybrid_handler']
            chunks, encrypted_metadata = handler.prepare_file_for_transfer(file_path)
            
            self.session['encrypted_chunks'] = chunks
            self.session['encrypted_metadata'] = encrypted_metadata
            
            print(f"[Controller] 🔒 File encrypted")
            print(f"[Controller] 📤 Ready to send: {metadata.filename}")
            print(f"             Size: {chunker._format_size(metadata.file_size)}")
            print(f"             Chunks: {metadata.total_chunks}")
            print(f"             Mode: {self.selected_mode.value if self.selected_mode else 'LAN'}")
            
            # Register with timeout manager
            timeout_type = TimeoutType.LAN if self.selected_mode == TransferMode.LAN else TimeoutType.WAN
            self.timeout_manager.register_transfer(
                transfer_id,
                timeout_type,
                metadata={'filename': metadata.filename}
            )
            
            if self.overlay:
                try:
                    self.overlay.set_state(UIState.COMPLETE, 
                        f"File ready!\n{metadata.filename}\n{chunker._format_size(metadata.file_size)}")
                except:
                    pass
                    
        except FileNotFoundError:
            print(f"[Controller] ❌ File not found: {file_path}")
        except Exception as e:
            print(f"[Controller] ❌ Error: {e}")
            
    def _send_link(self, url: str, transfer_id: str):
        """Send a link"""
        print(f"[Controller] 🔗 Link: {url}")
        
        if self.overlay:
            try:
                self.overlay.set_state(UIState.COMPLETE, f"Link ready:\n{url[:50]}...")
            except:
                pass
                
    def _start_download(self, transfer: IncomingTransfer):
        """Start download"""
        mode = DownloadMode.LAN if transfer.transfer_type == TransferType.LAN else DownloadMode.WAN
        
        print(f"[Controller] 📥 Starting download: {transfer.filename}")
        print(f"             Mode: {mode.value}")
        
        # For now, print success message
        # Full download will be implemented when both sides are connected
        if self.overlay:
            try:
                self.overlay.set_state(UIState.COMPLETE, f"Receiving:\n{transfer.filename}")
            except:
                pass
                    
    def run(self):
        """Run the main loop"""
        self.start()
        
        print("\n" + "=" * 50)
        print("🎯 PALMSYNC ACTIVE")
        print("=" * 50)
        print("\nGesture Guide:")
        print("  🤜 CLOSE FIST → Activate system")
        print("  👆 POINT LEFT → Select LAN")
        print("  👆 POINT RIGHT → Select WAN")
        print("  🖐️ OPEN PALM → Send file")
        print("\nCommands:")
        print("  s → Show session info")
        print("  d → Show discovered devices")
        print("  c → Show current content")
        print("  q → Quit\n")
        
        try:
            while self.is_running:
                cmd = input().strip().lower()
                
                if cmd == 'q':
                    break
                elif cmd == 's':
                    self._show_session_info()
                elif cmd == 'd':
                    self._show_devices()
                elif cmd == 'c':
                    self._show_content()
                    
        except KeyboardInterrupt:
            print("\n[Controller] Interrupted")
            
        self.stop()
        
    def _show_session_info(self):
        """Display current session information"""
        print(f"\n📋 Session Info:")
        print(f"   Device: {self.session['device_name']}")
        print(f"   IP: {self.session['local_ip']}")
        print(f"   State: {self.current_state.value}")
        print(f"   Mode: {self.selected_mode.value if self.selected_mode else 'None'}")
        
        content = self.session.get('active_content')
        if content:
            print(f"   Content: {content.display_name}")
            print(f"   Type: {content.content_type.value}")
            
        lan_devices = self.lan_discovery.get_devices_with_app()
        print(f"   LAN devices with app: {len(lan_devices)}")
        for d in lan_devices:
            print(f"      • {d.device_name} ({d.ip_address})")
            
    def _show_devices(self):
        """Show discovered devices"""
        print(f"\n📡 Network Devices:")
        
        app_devices = self.lan_discovery.get_devices_with_app()
        all_devices = self.lan_discovery.get_online_devices(app_only=False)
        
        if app_devices:
            print(f"\n   ✅ Ready for transfer ({len(app_devices)}):")
            for d in app_devices:
                print(f"      • {d.device_name} ({d.ip_address})")
        else:
            print(f"\n   ⚠️ No devices with app found")
            
        print(f"\n   🌐 All network devices ({len(all_devices)}):")
        for d in all_devices[:10]:
            icon = "✅" if d.has_our_app else "⭕"
            vendor = f" [{d.vendor}]" if d.vendor else ""
            print(f"      {icon} {d.device_name}{vendor} ({d.ip_address})")
            
    def _show_content(self):
        """Show current content"""
        content = self.content_detector.get_current_content()
        
        if content:
            print(f"\n📱 Current Content:")
            print(f"   Name: {content.display_name}")
            print(f"   Type: {content.content_type.value}")
            print(f"   Path: {content.source_path or 'N/A'}")
            if content.size_bytes:
                print(f"   Size: {content.size_formatted}")
            print(f"   Can send file: {content.can_send_file}")
            print(f"   Can send link: {content.can_send_link}")
        else:
            print(f"\n   No content detected")


# ============================================
# MAIN ENTRY POINT (UPDATED WITH CONFIG.JSON)
# ============================================

def main():
    print("=" * 60)
    print("🔐 PALMSYNC")
    print("   Touchless Gesture-Controlled File Transfer")
    print("   LAN + WAN | AES-256-GCM | Real-Time Relay")
    print("=" * 60)

    # Path to config file
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')

    # Load existing config if available
    config = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print("\n📂 Loaded saved configuration:")
            if config.get('device_name'):
                print(f"   Device: {config['device_name']}")
            if config.get('server_url'):
                print(f"   Server: {config['server_url']}")
            if config.get('username'):
                print(f"   User:   {config['username']}")
        except:
            config = {}

    # --- Ask for missing values ---
    changed = False

    # Device name
    if not config.get('device_name'):
        config['device_name'] = input("\nEnter your display name: ").strip()
        if not config['device_name']:
            config['device_name'] = 'PalmsyncUser'
        changed = True

    # Server URL
    if not config.get('server_url'):
        use_server = input("Use relay server? (y/n, default: y): ").strip().lower()
        if use_server != 'n':
            server_url = input("Relay server URL: ").strip()
            if server_url:
                config['server_url'] = server_url
                changed = True
            else:
                print("No URL provided – running in LAN‑only mode.")
                config['server_url'] = ''
        else:
            config['server_url'] = ''
            changed = True

    # Username (only if server is configured)
    if config.get('server_url') and not config.get('username'):
        config['username'] = input("Username for relay server: ").strip()
        if config['username']:
            changed = True
        else:
            print("No username – you can register/login from the menu later.")

    # Save if anything new was entered
    if changed:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        print(f"💾 Configuration saved to {config_path}")

    # Create controller and handle server login if applicable
    controller = SecureTransferController(config)

    if config.get('server_url') and controller.server_lookup:
        username = config.get('username')
        if username:
            print(f"\n📡 Auto‑connecting to relay server as '{username}'…")
            if not controller.server_lookup.login(username):
                # Try registering
                display_name = config.get('device_name', username)
                if controller.server_lookup.register(username, display_name):
                    print(f"✅ Registered as {username}")
                else:
                    print("⚠️ Could not authenticate – running LAN only.")
            controller.server_lookup.start()

    controller.run()
    print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()