"""
context/content_detector.py
AUTOMATIC real-time content detection
Detects current content whenever active window changes
"""

import os
import sys
import subprocess
import platform
from enum import Enum
from typing import Optional, Tuple, Callable
from dataclasses import dataclass
import threading
import time
import re

class ContentType(Enum):
    VIDEO_FILE = "video_file"
    IMAGE_FILE = "image_file"
    DOCUMENT_FILE = "document_file"
    AUDIO_FILE = "audio_file"
    OTHER_FILE = "other_file"
    STREAMING_LINK = "streaming_link"
    VIDEO_LINK = "video_link"
    WEB_LINK = "web_link"
    SCREENSHOT = "screenshot"
    UNKNOWN = "unknown"

@dataclass
class ContentInfo:
    """Information about detected content"""
    content_type: ContentType
    source_path: Optional[str] = None
    display_name: str = ""
    size_bytes: Optional[int] = None
    size_formatted: str = ""
    is_streaming: bool = False
    can_send_file: bool = True
    can_send_link: bool = True
    window_title: str = ""
    process_name: str = ""

class ContentDetector:
    def __init__(self, on_content_changed: Callable = None):
        """
        Initialize auto-detecting content detector
        
        Args:
            on_content_changed: Callback when content changes
                               callback(content_info)
        """
        self.on_content_changed = on_content_changed
        self.os_type = platform.system()
        
        # Current state
        self.current_content = None
        self.last_window_title = ""
        self.last_process_name = ""
        
        # Background monitoring
        self.is_running = False
        self.monitor_thread = None
        self.check_interval = 0.3  # Check every 300ms
        
        # Streaming platform detection
        self.streaming_domains = [
            'netflix.com', 'youtube.com', 'youtu.be',
            'primevideo.com', 'amazon.com/gp/video',
            'hotstar.com', 'disneyplus.com', 'hulu.com',
            'hbomax.com', 'peacocktv.com', 'paramountplus.com',
            'twitch.tv', 'vimeo.com', 'dailymotion.com',
            'spotify.com', 'applemusic.com', 'tidal.com',
            'zee5.com', 'sonyliv.com', 'mxplayer.in',
            'jiocinema.com', 'altbalaji.com', 'erosnow.com'
        ]
        
        # Video sharing domains
        self.video_domains = [
            'youtube.com', 'youtu.be', 'vimeo.com',
            'dailymotion.com', 'tiktok.com', 'instagram.com/reel',
            'facebook.com/watch', 'twitter.com', 'reddit.com/r/',
            'ted.com', 'bilibili.com'
        ]
        
        # File extensions mapping
        self.video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', 
                                 '.webm', '.m4v', '.mpeg', '.mpg', '.3gp', '.ogv']
        self.image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', 
                                 '.tiff', '.ico', '.heic', '.svg', '.raw']
        self.document_extensions = ['.pdf', '.doc', '.docx', '.txt', '.xlsx', '.xls',
                                    '.pptx', '.ppt', '.odt', '.rtf', '.csv', '.md']
        self.audio_extensions = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a',
                                 '.wma', '.opus', '.aiff', '.alac']
        
    def start_monitoring(self):
        """Start automatic background monitoring"""
        if self.is_running:
            return
            
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print("[ContentDetector] Auto-detection started - monitoring active window")
        
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        print("[ContentDetector] Stopped")
        
    def _monitor_loop(self):
        """Main monitoring loop - checks for window changes"""
        while self.is_running:
            try:
                # Get current active window
                window_title, process_name, process_id = self._get_active_window()
                
                # Check if window changed
                if (window_title != self.last_window_title or 
                    process_name != self.last_process_name):
                    
                    self.last_window_title = window_title
                    self.last_process_name = process_name
                    
                    # Detect content from new window
                    new_content = self._detect_content(window_title, process_name, process_id)
                    
                    # Only trigger if content actually changed
                    if self._content_changed(new_content):
                        self.current_content = new_content
                        
                        if self.on_content_changed:
                            self.on_content_changed(new_content)
                            
                        # Log detection
                        print(f"[ContentDetector] 📱 Detected: {new_content.display_name}")
                        print(f"                 Type: {new_content.content_type.value}")
                            
            except Exception as e:
                print(f"[ContentDetector] Error: {e}")
                
            time.sleep(self.check_interval)
            
    def _content_changed(self, new_content: ContentInfo) -> bool:
        """Check if content actually changed"""
        if not self.current_content:
            return True
            
        # Compare key fields
        return (new_content.source_path != self.current_content.source_path or
                new_content.content_type != self.current_content.content_type or
                new_content.display_name != self.current_content.display_name)
                
    def _detect_content(self, window_title: str, process_name: str, 
                        process_id: int) -> ContentInfo:
        """Detect content from window information"""
        
        process_lower = process_name.lower()
        
        # BROWSER DETECTION
        if self._is_browser(process_name):
            return self._detect_browser_content(process_name, window_title)
            
        # MEDIA PLAYER DETECTION
        if self._is_media_player(process_name):
            return self._detect_media_content(process_name, window_title)
            
        # DOCUMENT VIEWER DETECTION
        if self._is_document_viewer(process_name):
            return self._detect_document_content(process_name, window_title)
            
        # IMAGE VIEWER DETECTION
        if self._is_image_viewer(process_name):
            return self._detect_image_content(process_name, window_title)
            
        # FILE EXPLORER DETECTION
        if self._is_file_explorer(process_name):
            return self._detect_explorer_content(window_title)
            
        # TEXT EDITOR DETECTION
        if self._is_text_editor(process_name):
            return self._detect_text_editor_content(process_name, window_title)
            
        # ANY OTHER APP - Try to extract file from window title
        file_path = self._extract_file_from_title(window_title)
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        # FALLBACK - Screenshot mode
        return ContentInfo(
            content_type=ContentType.SCREENSHOT,
            display_name=f"Screenshot: {window_title[:40]}" if window_title else "Screen Capture",
            window_title=window_title,
            process_name=process_name,
            can_send_file=True,
            can_send_link=False
        )
        
    def _is_browser(self, process_name: str) -> bool:
        browsers = ['chrome', 'firefox', 'safari', 'edge', 'opera', 
                    'brave', 'vivaldi', 'chromium', 'msedge', 'iexplore']
        process_lower = process_name.lower()
        return any(browser in process_lower for browser in browsers)
        
    def _is_media_player(self, process_name: str) -> bool:
        players = ['vlc', 'mpv', 'mplayer', 'totem', 'banshee', 'rhythmbox', 
                   'wmplayer', 'quicktime', 'itunes', 'spotify', 'groove',
                   'movies & tv', 'film & tv', 'kodi', 'plex', 'jellyfin']
        process_lower = process_name.lower()
        return any(player in process_lower for player in players)
        
    def _is_document_viewer(self, process_name: str) -> bool:
        viewers = ['acrobat', 'adobe', 'foxit', 'evince', 'okular', 'preview',
                   'word', 'excel', 'powerpoint', 'libreoffice', 'openoffice',
                   'pages', 'numbers', 'keynote', 'wps', 'sumatra', 'calibre']
        process_lower = process_name.lower()
        return any(viewer in process_lower for viewer in viewers)
        
    def _is_image_viewer(self, process_name: str) -> bool:
        viewers = ['photos', 'gallery', 'irfanview', 'xnview', 'gthumb', 'eog',
                   'gwenview', 'photoshop', 'gimp', 'paint', 'preview', 'picasa',
                   'digikam', 'photoscape', 'paint.net', 'affinity']
        process_lower = process_name.lower()
        return any(viewer in process_lower for viewer in viewers)
        
    def _is_file_explorer(self, process_name: str) -> bool:
        explorers = ['explorer', 'finder', 'nautilus', 'dolphin', 'thunar',
                     'pcmanfm', 'caja', 'nemo', 'files']
        process_lower = process_name.lower()
        return any(explorer in process_lower for explorer in explorers)
        
    def _is_text_editor(self, process_name: str) -> bool:
        editors = ['notepad', 'textedit', 'gedit', 'kate', 'sublime', 'atom',
                   'vscode', 'code', 'vim', 'nano', 'brackets', 'notepad++']
        process_lower = process_name.lower()
        return any(editor in process_lower for editor in editors)
        
    def _detect_browser_content(self, process_name: str, window_title: str) -> ContentInfo:
        """Detect content from browser"""
        url = self._get_browser_url(process_name)
        
        if url:
            # Check for streaming
            if self._is_streaming_url(url):
                platform_name = self._extract_platform_name(url)
                return ContentInfo(
                    content_type=ContentType.STREAMING_LINK,
                    source_path=url,
                    display_name=f"🎬 {platform_name}: {window_title}" if window_title else url,
                    is_streaming=True,
                    can_send_file=False,
                    can_send_link=True,
                    window_title=window_title,
                    process_name=process_name
                )
            # Check for video
            elif self._is_video_url(url):
                return ContentInfo(
                    content_type=ContentType.VIDEO_LINK,
                    source_path=url,
                    display_name=f"📹 {window_title}" if window_title else url,
                    can_send_file=False,
                    can_send_link=True,
                    window_title=window_title,
                    process_name=process_name
                )
            # Regular webpage
            else:
                title = window_title.split(' - ')[0] if ' - ' in window_title else window_title
                return ContentInfo(
                    content_type=ContentType.WEB_LINK,
                    source_path=url,
                    display_name=f"🌐 {title[:50]}" if title else url[:50],
                    can_send_file=False,
                    can_send_link=True,
                    window_title=window_title,
                    process_name=process_name
                )
                
        # No URL found - screenshot mode
        return ContentInfo(
            content_type=ContentType.SCREENSHOT,
            display_name=f"📸 Browser: {window_title[:40]}",
            can_send_file=True,
            can_send_link=False,
            window_title=window_title,
            process_name=process_name
        )
        
    def _detect_media_content(self, process_name: str, window_title: str) -> ContentInfo:
        """Detect media file from player"""
        file_path = self._extract_file_from_title(window_title)
        
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        # Try to extract from process command line
        file_path = self._get_file_from_process(process_name)
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        # Fallback - use window title
        ext = os.path.splitext(window_title)[1].lower()
        if ext in self.video_extensions:
            content_type = ContentType.VIDEO_FILE
            emoji = "🎬"
        elif ext in self.audio_extensions:
            content_type = ContentType.AUDIO_FILE
            emoji = "🎵"
        else:
            content_type = ContentType.VIDEO_FILE
            emoji = "🎬"
            
        return ContentInfo(
            content_type=content_type,
            display_name=f"{emoji} {window_title[:60]}",
            can_send_file=False,
            can_send_link=True,
            window_title=window_title,
            process_name=process_name
        )
        
    def _detect_document_content(self, process_name: str, window_title: str) -> ContentInfo:
        """Detect document file"""
        file_path = self._extract_file_from_title(window_title)
        
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        # Check process command line
        file_path = self._get_file_from_process(process_name)
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        return ContentInfo(
            content_type=ContentType.DOCUMENT_FILE,
            display_name=f"📄 {window_title[:60]}",
            can_send_file=False,
            can_send_link=False,
            window_title=window_title,
            process_name=process_name
        )
        
    def _detect_image_content(self, process_name: str, window_title: str) -> ContentInfo:
        """Detect image file"""
        file_path = self._extract_file_from_title(window_title)
        
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        return ContentInfo(
            content_type=ContentType.IMAGE_FILE,
            display_name=f"🖼️ {window_title[:60]}",
            can_send_file=False,
            can_send_link=False,
            window_title=window_title,
            process_name=process_name
        )
        
    def _detect_explorer_content(self, window_title: str) -> ContentInfo:
        """Detect selected file from explorer"""
        selected = self._get_selected_file()
        
        if selected and os.path.exists(selected):
            return self._create_file_content(selected, window_title, "explorer")
            
        # Show current folder
        folder = self._get_current_folder()
        if folder:
            return ContentInfo(
                content_type=ContentType.OTHER_FILE,
                display_name=f"📁 Folder: {os.path.basename(folder)}",
                source_path=folder,
                can_send_file=False,
                can_send_link=False,
                window_title=window_title,
                process_name="explorer"
            )
            
        return ContentInfo(
            content_type=ContentType.SCREENSHOT,
            display_name="📸 File Explorer",
            can_send_file=True,
            can_send_link=False,
            window_title=window_title,
            process_name="explorer"
        )
        
    def _detect_text_editor_content(self, process_name: str, window_title: str) -> ContentInfo:
        """Detect file from text editor"""
        file_path = self._extract_file_from_title(window_title)
        
        if file_path and os.path.exists(file_path):
            return self._create_file_content(file_path, window_title, process_name)
            
        return ContentInfo(
            content_type=ContentType.DOCUMENT_FILE,
            display_name=f"📝 {window_title[:60]}" if window_title else "Text Document",
            can_send_file=False,
            can_send_link=False,
            window_title=window_title,
            process_name=process_name
        )
        
    def _create_file_content(self, file_path: str, window_title: str, 
                             process_name: str) -> ContentInfo:
        """Create ContentInfo for a file"""
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext in self.video_extensions:
            content_type = ContentType.VIDEO_FILE
            emoji = "🎬"
        elif ext in self.image_extensions:
            content_type = ContentType.IMAGE_FILE
            emoji = "🖼️"
        elif ext in self.document_extensions:
            content_type = ContentType.DOCUMENT_FILE
            emoji = "📄"
        elif ext in self.audio_extensions:
            content_type = ContentType.AUDIO_FILE
            emoji = "🎵"
        else:
            content_type = ContentType.OTHER_FILE
            emoji = "📁"
            
        return ContentInfo(
            content_type=content_type,
            source_path=file_path,
            display_name=f"{emoji} {file_name}",
            size_bytes=file_size,
            size_formatted=self.format_size(file_size),
            can_send_file=True,
            can_send_link=False,
            window_title=window_title,
            process_name=process_name
        )
        
    def _extract_file_from_title(self, title: str) -> Optional[str]:
        """Extract file path from window title"""
        if not title:
            return None
            
        # Windows path pattern
        windows_pattern = r'([A-Za-z]:[\\/][^\s*?:"<>|]+\.\w+)'
        match = re.search(windows_pattern, title)
        if match:
            return match.group(1)
            
        # Unix path pattern
        unix_pattern = r'(/[^\s*?:"<>|]+\.\w+)'
        match = re.search(unix_pattern, title)
        if match:
            return match.group(1)
            
        # Just filename with extension
        file_pattern = r'([^\s*?:"<>|]+\.\w{2,5})'
        match = re.search(file_pattern, title)
        if match:
            filename = match.group(1)
            # Check common locations
            common_paths = [
                os.path.expanduser("~/Downloads"),
                os.path.expanduser("~/Documents"),
                os.path.expanduser("~/Videos"),
                os.path.expanduser("~/Pictures"),
                os.path.expanduser("~/Desktop"),
                "C:/Users/Public/Videos",
                "C:/Users/Public/Pictures",
            ]
            for path in common_paths:
                full_path = os.path.join(path, filename)
                if os.path.exists(full_path):
                    return full_path
                    
        return None
        
    def _is_streaming_url(self, url: str) -> bool:
        url_lower = url.lower()
        return any(domain in url_lower for domain in self.streaming_domains)
        
    def _is_video_url(self, url: str) -> bool:
        url_lower = url.lower()
        return any(domain in url_lower for domain in self.video_domains)
        
    def _extract_platform_name(self, url: str) -> str:
        url_lower = url.lower()
        for domain in self.streaming_domains:
            if domain in url_lower:
                return domain.replace('.com', '').replace('www.', '').title()
        return "Streaming"
        
    def format_size(self, size_bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"
        
    # ===== OS-SPECIFIC METHODS =====
    
    def _get_active_window(self) -> Tuple[str, str, int]:
        """Get current active window - cross-platform"""
        try:
            if self.os_type == "Windows":
                return self._get_active_window_windows()
            elif self.os_type == "Darwin":
                return self._get_active_window_macos()
            else:
                return self._get_active_window_linux()
        except:
            return "", "", 0
            
    def _get_active_window_windows(self) -> Tuple[str, str, int]:
        try:
            import win32gui
            import win32process
            import psutil
            
            hwnd = win32gui.GetForegroundWindow()
            window_title = win32gui.GetWindowText(hwnd)
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            
            try:
                process = psutil.Process(pid)
                process_name = process.name()
            except:
                process_name = ""
                
            return window_title, process_name, pid
        except ImportError:
            return self._fallback_active_window()
            
    def _get_active_window_macos(self) -> Tuple[str, str, int]:
        try:
            script = '''
            tell application "System Events"
                set frontApp to name of first application process whose frontmost is true
                set windowTitle to ""
                try
                    tell process frontApp
                        set windowTitle to name of front window
                    end tell
                end try
                return frontApp & "|" & windowTitle
            end tell
            '''
            result = subprocess.run(['osascript', '-e', script], 
                                   capture_output=True, text=True, timeout=1)
            if result.returncode == 0:
                parts = result.stdout.strip().split('|')
                app = parts[0] if len(parts) > 0 else ""
                title = parts[1] if len(parts) > 1 else ""
                return title, app, 0
        except:
            pass
        return self._fallback_active_window()
        
    def _get_active_window_linux(self) -> Tuple[str, str, int]:
        try:
            result = subprocess.run(['xdotool', 'getactivewindow', 'getwindowname'],
                                   capture_output=True, text=True, timeout=1)
            if result.returncode == 0:
                return result.stdout.strip(), "", 0
        except:
            pass
        return self._fallback_active_window()
        
    def _fallback_active_window(self) -> Tuple[str, str, int]:
        return "Unknown", "unknown", 0
        
    def _get_browser_url(self, process_name: str) -> Optional[str]:
        """Get current URL - placeholder for browser extension integration"""
        return None
        
    def _get_file_from_process(self, process_name: str) -> Optional[str]:
        """Try to get file from process command line"""
        return None
        
    def _get_selected_file(self) -> Optional[str]:
        """Get selected file from explorer"""
        return None
        
    def _get_current_folder(self) -> Optional[str]:
        """Get current folder path"""
        return None
        
    def get_current_content(self) -> Optional[ContentInfo]:
        """Get the most recently detected content"""
        return self.current_content


# ============================================
# TEST FUNCTION - AUTO DETECTION DEMO
# ============================================

def test_auto_detection():
    """Test automatic content detection"""
    
    def on_content_changed(content: ContentInfo):
        """Callback when content changes"""
        print("\n" + "=" * 50)
        print(f"📱 NEW CONTENT DETECTED!")
        print(f"   Type: {content.content_type.value}")
        print(f"   Name: {content.display_name}")
        if content.source_path:
            print(f"   Path: {content.source_path}")
        if content.size_bytes:
            print(f"   Size: {content.size_formatted}")
        print(f"   Window: {content.window_title}")
        print(f"   App: {content.process_name}")
        print("=" * 50 + "\n")
        
    detector = ContentDetector(on_content_changed=on_content_changed)
    detector.start_monitoring()
    
    print("🔍 AUTO-DETECTION ACTIVE")
    print("   Switch between different apps/files and watch detection happen automatically!")
    print("   - Open a video in VLC → Detected")
    print("   - Browse Netflix → Detected")
    print("   - Open a PDF → Detected")
    print("   - View an image → Detected")
    print("\nPress Ctrl+C to stop...\n")
    
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping...")
        detector.stop_monitoring()


if __name__ == "__main__":
    test_auto_detection()