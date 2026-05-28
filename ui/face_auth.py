"""
ui/face_auth.py
Face authentication for secure transfers
Verifies sender/receiver identity before allowing transfer
"""

import cv2
import face_recognition
import numpy as np
import os
import pickle
import time
import threading
from typing import Optional, Callable, List
from dataclasses import dataclass
from enum import Enum

class AuthStatus(Enum):
    PENDING = "pending"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    FAILED = "failed"
    TIMEOUT = "timeout"

@dataclass
class FaceProfile:
    """Registered face profile"""
    user_id: str
    name: str
    face_encodings: List[np.ndarray]
    created_at: float
    last_verified: float
    verification_count: int = 0

class FaceAuthenticator:
    """
    Face authentication using facial recognition
    Ensures only authorized users can send/receive
    """
    
    VERIFICATION_TIMEOUT = 5.0  # Seconds to verify face
    CONFIDENCE_THRESHOLD = 0.6  # Face match threshold
    REQUIRED_MATCHES = 3  # Consecutive matches needed
    
    def __init__(self, profiles_dir: str = "face_profiles"):
        self.profiles_dir = profiles_dir
        os.makedirs(profiles_dir, exist_ok=True)
        
        # Load registered profiles
        self.profiles: List[FaceProfile] = []
        self._load_profiles()
        
        # Current verification state
        self.status = AuthStatus.PENDING
        self.current_user: Optional[FaceProfile] = None
        self.match_count = 0
        self.verification_start_time = 0
        
        # Callbacks
        self.on_verified = None
        self.on_failed = None
        self.on_timeout = None
        
        # Threading
        self.is_running = False
        self.verify_thread = None
        
        # Face detection
        self.face_locations = []
        self.face_encodings = []
        
        print(f"[FaceAuth] ✅ Loaded {len(self.profiles)} registered face profiles")
        
    def register_face(self, user_id: str, name: str, 
                      capture_seconds: int = 5) -> Optional[FaceProfile]:
        """
        Register a new face profile
        
        Args:
            user_id: Unique user ID
            name: Display name
            capture_seconds: How long to capture face samples
            
        Returns:
            FaceProfile if successful
        """
        print(f"\n[FaceAuth] 📸 Registering face for {name}")
        print("            Please look at the camera...")
        
        encodings = []
        cap = cv2.VideoCapture(0)
        start_time = time.time()
        
        while time.time() - start_time < capture_seconds:
            ret, frame = cap.read()
            if not ret:
                continue
                
            # Detect faces
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            face_locations = face_recognition.face_locations(rgb_frame)
            face_encodings = face_recognition.face_encodings(rgb_frame, face_locations)
            
            # Draw rectangle around face
            for (top, right, bottom, left) in face_locations:
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)
                
            # Show progress
            progress = (time.time() - start_time) / capture_seconds
            cv2.putText(frame, f"Registering: {int(progress * 100)}%", 
                       (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            cv2.imshow("Face Registration", frame)
            
            if face_encodings:
                encodings.append(face_encodings[0])
                
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
                
        cap.release()
        cv2.destroyAllWindows()
        
        if len(encodings) >= 3:
            profile = FaceProfile(
                user_id=user_id,
                name=name,
                face_encodings=encodings,
                created_at=time.time(),
                last_verified=time.time()
            )
            
            self.profiles.append(profile)
            self._save_profile(profile)
            
            print(f"[FaceAuth] ✅ Face registered for {name}")
            return profile
        else:
            print(f"[FaceAuth] ❌ Registration failed - not enough face samples")
            return None
            
    def verify_face(self, on_verified: Callable = None,
                    on_failed: Callable = None,
                    on_timeout: Callable = None) -> bool:
        """
        Start face verification
        
        Returns:
            True if verification started
        """
        if not self.profiles:
            print("[FaceAuth] ⚠️ No registered profiles!")
            return False
            
        self.on_verified = on_verified
        self.on_failed = on_failed
        self.on_timeout = on_timeout
        
        self.status = AuthStatus.VERIFYING
        self.match_count = 0
        self.verification_start_time = time.time()
        
        self.is_running = True
        self.verify_thread = threading.Thread(target=self._verification_loop, daemon=True)
        self.verify_thread.start()
        
        print("[FaceAuth] 🔍 Verifying identity...")
        print("            Please look at the camera")
        
        return True
        
    def _verification_loop(self):
        """Main verification loop"""
        cap = cv2.VideoCapture(0)
        
        while self.is_running and self.status == AuthStatus.VERIFYING:
            ret, frame = cap.read()
            if not ret:
                continue
                
            # Check timeout
            if time.time() - self.verification_start_time > self.VERIFICATION_TIMEOUT:
                self.status = AuthStatus.TIMEOUT
                print("[FaceAuth] ⏰ Verification timeout")
                if self.on_timeout:
                    self.on_timeout()
                break
                
            # Detect faces
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            face_locations = face_recognition.face_locations(rgb_frame)
            face_encodings = face_recognition.face_encodings(rgb_frame, face_locations)
            
            # Draw face boxes
            for (top, right, bottom, left) in face_locations:
                color = (0, 255, 0) if self.match_count > 0 else (0, 0, 255)
                cv2.rectangle(frame, (left, top), (right, bottom), color, 2)
                
            # Check against registered profiles
            if face_encodings:
                matched_profile = self._match_face(face_encodings[0])
                
                if matched_profile:
                    self.match_count += 1
                    
                    if self.match_count >= self.REQUIRED_MATCHES:
                        self.status = AuthStatus.VERIFIED
                        self.current_user = matched_profile
                        matched_profile.last_verified = time.time()
                        matched_profile.verification_count += 1
                        
                        print(f"[FaceAuth] ✅ VERIFIED: {matched_profile.name}")
                        
                        if self.on_verified:
                            self.on_verified(matched_profile)
                        break
                else:
                    self.match_count = 0
                    
            # Show status on frame
            remaining = self.VERIFICATION_TIMEOUT - (time.time() - self.verification_start_time)
            status_text = f"Verifying... {remaining:.1f}s"
            
            if self.match_count > 0:
                progress = self.match_count / self.REQUIRED_MATCHES
                status_text = f"Matched! {int(progress * 100)}%"
                color = (0, 255, 0)
            else:
                color = (0, 0, 255)
                
            cv2.putText(frame, status_text, (10, 30), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
            
            cv2.imshow("Face Authentication", frame)
            
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
                
        cap.release()
        cv2.destroyAllWindows()
        
        if self.status == AuthStatus.VERIFYING:
            self.status = AuthStatus.FAILED
            if self.on_failed:
                self.on_failed()
                
    def _match_face(self, face_encoding: np.ndarray) -> Optional[FaceProfile]:
        """Match face against registered profiles"""
        for profile in self.profiles:
            for known_encoding in profile.face_encodings:
                # Compare faces
                face_distances = face_recognition.face_distance([known_encoding], face_encoding)
                
                if face_distances[0] < self.CONFIDENCE_THRESHOLD:
                    return profile
                    
        return None
        
    def _save_profile(self, profile: FaceProfile):
        """Save face profile to disk"""
        profile_path = os.path.join(self.profiles_dir, f"{profile.user_id}.pkl")
        
        with open(profile_path, 'wb') as f:
            pickle.dump(profile, f)
            
    def _load_profiles(self):
        """Load saved face profiles"""
        for filename in os.listdir(self.profiles_dir):
            if filename.endswith('.pkl'):
                profile_path = os.path.join(self.profiles_dir, filename)
                
                try:
                    with open(profile_path, 'rb') as f:
                        profile = pickle.load(f)
                        self.profiles.append(profile)
                except:
                    pass
                    
    def get_current_user(self) -> Optional[FaceProfile]:
        """Get currently verified user"""
        return self.current_user
        
    def is_verified(self) -> bool:
        """Check if user is verified"""
        return self.status == AuthStatus.VERIFIED
        
    def reset(self):
        """Reset verification state"""
        self.status = AuthStatus.PENDING
        self.current_user = None
        self.match_count = 0
        self.is_running = False