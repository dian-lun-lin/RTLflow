// -*- mode: C++; c-file-style: "cc-mode" -*-
//*************************************************************************
// DESCRIPTION: Verilator: Hash calculation
//
// Code available from: https://verilator.org
//
//*************************************************************************
//
// Copyright 2003-2021 by Wilson Snyder. This program is free software; you
// can redistribute it and/or modify it under the terms of either the GNU
// Lesser General Public License Version 3 or the Perl Artistic License
// Version 2.0.
// SPDX-License-Identifier: LGPL-3.0-only OR Artistic-2.0
//
//*************************************************************************

#ifndef VERILATOR_V3HASH_H_
#define VERILATOR_V3HASH_H_

#include <cstdint>
#include <string>

//######################################################################
// V3Hash -- Generic hashing

class V3Hash final {
    uint32_t m_value;  // The 32-bit hash value.

public:
    // CONSTRUCTORS
    V3Hash()
        : m_value{0} {}
    explicit V3Hash(uint32_t val)
        : m_value{val + 0x9e3779b9} {}  // This is the same as 'V3Hash() + val'
    explicit V3Hash(int32_t val)
        : m_value{static_cast<uint32_t>(val)} {}
    explicit V3Hash(size_t val)
        : m_value{static_cast<uint32_t>(val)} {}
    explicit V3Hash(const std::string& val);

    // METHODS
    uint32_t value() const { return m_value; }

    // OPERATORS
    // Comparisons
    bool operator==(const V3Hash& rh) const { return m_value == rh.m_value; }
    bool operator!=(const V3Hash& rh) const { return m_value != rh.m_value; }
    bool operator<(const V3Hash& rh) const { return m_value < rh.m_value; }

    // '+' combines hashes
    V3Hash operator+(const V3Hash& that) const {
        return V3Hash(m_value ^ (that.m_value + 0x9e3779b9 + (m_value << 6) + (m_value >> 2)));
    }
    V3Hash operator+(uint32_t value) const { return *this + V3Hash(value); }
    V3Hash operator+(int32_t value) const { return *this + V3Hash(value); }
    V3Hash operator+(size_t value) const { return *this + V3Hash(value); }
    V3Hash operator+(const std::string& value) const { return *this + V3Hash(value); }

    // '+=' combines in place
    V3Hash& operator+=(const V3Hash& that) { return *this = *this + that; }
    V3Hash& operator+=(uint32_t value) { return *this += V3Hash(value); }
    V3Hash& operator+=(int32_t value) { return *this += V3Hash(value); }
    V3Hash& operator+=(size_t value) { return *this += V3Hash(value); }
    V3Hash& operator+=(const std::string& that) { return *this += V3Hash(that); }
};

std::ostream& operator<<(std::ostream& os, const V3Hash& rhs);

#endif  // Guard
