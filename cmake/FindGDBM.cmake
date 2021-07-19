# Copyright (C) 2007-2009 LuaDist.
# Created by Peter Kapec <kapecp@gmail.com>
# Redistribution and use of this file is allowed according to the terms of the MIT license.
# For details see the COPYRIGHT file distributed with LuaDist.
#	Note:
#		Searching headers and libraries is very simple and is NOT as powerful as scripts
#		distributed with CMake, because LuaDist defines directories to search for.
#		Everyone is encouraged to contact the author with improvements. Maybe this file
#		becomes part of CMake distribution sometimes.

# - Find gdbm
# Find the native GDBM headers and libraries.
#
# GDBM_INCLUDE_DIRS	- where to find gdbm.h.
# GDBM_LIBRARIES	- List of libraries when using mapm.
# GDBM_FOUND	- True if mapm found.

# Look for the header file.
FIND_PATH(GDBM_INCLUDE_DIR NAMES gdbm.h)

# Look for the library.
FIND_LIBRARY(GDBM_LIBRARY NAMES gdbm)

# Handle the QUIETLY and REQUIRED arguments and set GDBM_FOUND to TRUE if all listed variables are TRUE.
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GDBM DEFAULT_MSG GDBM_LIBRARY GDBM_INCLUDE_DIR)

# Copy the results to the output variables.
IF(GDBM_FOUND)
	SET(GDBM_LIBRARIES ${GDBM_LIBRARY})
	SET(GDBM_INCLUDE_DIRS ${GDBM_INCLUDE_DIR})
ELSE(GDBM_FOUND)
	SET(GDBM_LIBRARIES)
	SET(GDBM_INCLUDE_DIRS)
ENDIF(GDBM_FOUND)

MARK_AS_ADVANCED(GDBM_INCLUDE_DIRS GDBM_LIBRARIES)
