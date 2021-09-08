package com.lagou.edu.mvcframework.security;

import java.util.Objects;

/**
 * `SecurityRole` is a wrapper around permitted role that can access the method under the handler
 */
public class SecurityRole {
    private String permittedUserName;

    public SecurityRole(String permittedUserName) {
        this.permittedUserName = permittedUserName;
    }

    public String getPermittedUserName() {
        return permittedUserName;
    }

    public void setPermittedUserName(String permittedUserName) {
        this.permittedUserName = permittedUserName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecurityRole that = (SecurityRole) o;
        return permittedUserName.equals(that.permittedUserName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(permittedUserName);
    }
}
