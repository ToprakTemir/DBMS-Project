

class KeyConstraintViolation(Exception):
    """Raised when a key constraint is violated."""
    def __init__(self, message="Key constraint violation occurred."):
        super().__init__(message)