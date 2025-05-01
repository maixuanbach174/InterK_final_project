from dbcsv.app.security.auth import auth_manager
from fastapi import Depends

current_user_dependency = Depends(auth_manager.get_current_user)
