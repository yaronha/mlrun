from enum import Flag


class Permission(Flag):
    # Don't use "auto" since these are stored in the database
    READ = 1 << 0
    WRITE = 1 << 1
    ADMIN = 1 << 2


class PermissionStore:
    """User permissions"""
    def __init__(self):
        self._user_perms = {}

    def set(self, project, user, perm: Permission):
        self._user_perms[(project, user)] = perm

    def match(self, project, user, mask):
        perm = self._user_perms.get((project, user), 0)
        return perm & mask == mask
