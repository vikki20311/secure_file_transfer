"""
server/sms_service.py
PalmSync SMS Service via Twilio
Free trial: 1900 SMS, then ~$0.008/SMS
"""

import logging
from typing import Optional

logger = logging.getLogger('PalmSync.SMS')

# Try to import Twilio
try:
    from twilio.rest import Client
    from twilio.base.exceptions import TwilioRestException
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False
    logger.warning("⚠️ Twilio not installed. Run: pip install twilio")

class SMSService:
    """
    SMS Service for sending OTP codes
    Uses Twilio free trial initially
    """
    
    def __init__(self, account_sid: str = None, auth_token: str = None, from_number: str = None):
        """
        Initialize SMS service
        
        Args:
            account_sid: Twilio Account SID (from console.twilio.com)
            auth_token: Twilio Auth Token
            from_number: Your Twilio phone number (+1XXXXXXXXXX)
        """
        # Use provided or default credentials
        self.account_sid = account_sid or "ACxxxxxxxxxxxx"  # ← REPLACE THIS
        self.auth_token = auth_token or "xxxxxxxxxxxxxxxx"   # ← REPLACE THIS
        self.from_number = from_number or "+15550100"         # ← REPLACE THIS
        
        self.client = None
        self.is_available = False
        
        # Try to connect
        self._connect()
        
    def _connect(self):
        """Connect to Twilio"""
        if not TWILIO_AVAILABLE:
            logger.warning("⚠️ Twilio package not installed")
            return
            
        if 'xxxxxxxx' in self.account_sid:
            logger.warning("⚠️ Using placeholder credentials. Update with real ones!")
            logger.info("🔧 DEV MODE: OTPs will be printed to console")
            return
            
        try:
            self.client = Client(self.account_sid, self.auth_token)
            
            # Test connection
            account = self.client.api.account(self.account_sid).fetch()
            logger.info(f"✅ Twilio connected! Account: {account.friendly_name}")
            self.is_available = True
            
        except TwilioRestException as e:
            logger.error(f"❌ Twilio connection failed: {e}")
            logger.info("🔧 DEV MODE: Falling back to console OTP")
        except Exception as e:
            logger.error(f"❌ Twilio error: {e}")
            
    def send_otp(self, phone_number: str, otp: str) -> dict:
        """
        Send OTP via SMS
        
        Args:
            phone_number: Recipient phone (e.g., "8882525519" or "+918882525519")
            otp: 6-digit OTP code
            
        Returns:
            {'sent': True/False, 'method': 'sms'/'console', 'error': None/str}
        """
        # Clean phone number
        phone = self._clean_phone(phone_number)
        
        # Try SMS if Twilio is available
        if self.client and self.is_available:
            try:
                message = self.client.messages.create(
                    body=f"🔐 Your PalmSync verification code is: {otp}\n\nValid for 5 minutes.\n\nIf you didn't request this, please ignore.",
                    from_=self.from_number,
                    to=phone
                )
                
                logger.info(f"📱 SMS sent to {phone}: SID={message.sid}")
                
                return {
                    'sent': True,
                    'method': 'sms',
                    'message_sid': message.sid,
                    'to': phone
                }
                
            except TwilioRestException as e:
                logger.error(f"❌ SMS failed: {e}")
                
                # Trial account can only send to verified numbers
                if 'not a valid phone number' in str(e) or 'unverified' in str(e):
                    logger.info("💡 TIP: Trial accounts can only send to verified numbers.")
                    logger.info("   Verify your number at: https://console.twilio.com/us1/develop/phone-numbers/manage/verified")
                
                # Fall back to console
                return self._console_otp(phone, otp, str(e))
        else:
            # No Twilio - use console
            return self._console_otp(phone, otp, "Twilio not configured")
            
    def _console_otp(self, phone: str, otp: str, reason: str) -> dict:
        """Fallback: Print OTP to console (development only)"""
        logger.info("=" * 50)
        logger.info(f"🔧 DEV MODE - SMS not sent ({reason})")
        logger.info(f"📱 Phone: {phone}")
        logger.info(f"🔑 OTP:   {otp}")
        logger.info(f"⏰ Valid: 5 minutes")
        logger.info("=" * 50)
        
        return {
            'sent': True,
            'method': 'console',
            'error': reason,
            'to': phone,
            'dev_otp': otp  # Client can show this in development
        }
        
    def send_welcome(self, phone_number: str, username: str) -> bool:
        """Send welcome message after registration"""
        phone = self._clean_phone(phone_number)
        
        if self.client and self.is_available:
            try:
                self.client.messages.create(
                    body=f"🎉 Welcome to PalmSync, @{username}!\n\nShare files with simple hand gestures. Get started!",
                    from_=self.from_number,
                    to=phone
                )
                return True
            except:
                return False
        return False
        
    def _clean_phone(self, phone: str) -> str:
        """Format phone number to E.164 format"""
        # Remove spaces, dashes, etc.
        phone = ''.join(c for c in phone if c.isdigit() or c == '+')
        
        # Add India code if no country code
        if not phone.startswith('+'):
            if len(phone) == 10:  # Indian mobile
                phone = f"+91{phone}"
            elif len(phone) > 10:
                phone = f"+{phone}"
                
        return phone
        
    def verify_number(self, phone: str) -> bool:
        """Check if number is valid"""
        phone = self._clean_phone(phone)
        
        if self.client and self.is_available:
            try:
                # Twilio lookup
                lookup = self.client.lookups.v2.phone_numbers(phone).fetch()
                return lookup.valid
            except:
                pass
                
        # Basic validation
        digits = ''.join(c for c in phone if c.isdigit())
        return len(digits) >= 10
        
    def get_balance(self) -> Optional[float]:
        """Get remaining trial balance"""
        if self.client and self.is_available:
            try:
                balance = self.client.api.account(self.account_sid).balance.fetch()
                return float(balance.balance)
            except:
                pass
        return None


# ============================================
# QUICK TEST
# ============================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("📱 PalmSync SMS Service Test")
    print("=" * 40)
    
    # Create service (uses placeholders, will print to console)
    sms = SMSService()
    
    # Test OTP
    result = sms.send_otp("8882525519", "472891")
    
    print(f"\nResult: {result}")
    
    # Check if Twilio connected
    if sms.is_available:
        balance = sms.get_balance()
        print(f"💰 Trial balance: ${balance}")
    else:
        print("\n💡 To use real SMS:")
        print("   1. Sign up at https://www.twilio.com")
        print("   2. Get Account SID and Auth Token")
        print("   3. Update sms_service.py with credentials")
        print("   4. Verify your phone number in Twilio console")