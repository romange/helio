# Reusable, retrying downloader for third-party URL/tarball dependencies. This
# script should be used during build time.
#
# ExternalProject_Add has no configurable retry count for URL downloads, and a
# single transient network flake fails the whole build. This script is meant to
# be used as a custom DOWNLOAD_COMMAND (or invoked directly at configure time)
# so that every URL/tarball dependency shares one retry policy instead of each
# dependency re-implementing its own.
#
# Invoke via:
#   cmake -DURL=<url> [-DURL_FALLBACK=<url>] -DDEST=<file>
#         [-DEXPECTED_HASH=<sha256>] [-DEXTRACT_DIR=<dir>]
#         [-DMIN_SIZE=<bytes>] [-DRETRIES=<n>]
#         [-DBACKOFF=<seconds>] [-DBACKOFF_MAX=<seconds>]
#         -P download_retry.cmake
#
# Parameters:
#   URL           (required) URL to download from.
#   DEST          (required) Path where the downloaded file is written.
#   URL_FALLBACK  (optional) Single fallback URL tried after URL on each attempt.
#   EXPECTED_HASH (optional) Expected SHA256 of the download, as a raw hex digest
#                            or an "SHA256=<hex>" string. A mismatch is retried.
#   EXTRACT_DIR   (optional) If set, the archive is extracted here (stripping a
#                            single leading directory) after a successful download.
#   MIN_SIZE      (optional) Minimum acceptable download size in bytes; a smaller
#                            file is treated as a failure and retried.
#   RETRIES       (optional) Maximum number of download attempts. Default 10.
#   BACKOFF       (optional) Initial sleep in seconds between attempts. Default 5.
#   BACKOFF_MAX   (optional) Upper bound in seconds for the backoff sleep, which
#                            doubles after each failed attempt. Default 60.
#
# Behavior:
#   * Retries the download up to RETRIES times (default 10). Between attempts it
#     sleeps with exponential backoff starting at BACKOFF seconds (default 5) and
#     doubling up to BACKOFF_MAX seconds (default 60): 5, 10, 20, 40, 60, 60...
#     After the last attempt it fails with a clear error.
#   * On each attempt URL is tried first, then URL_FALLBACK (if given).
#   * If EXPECTED_HASH is set, the SHA256 is verified and a mismatch is retried.
#   * If MIN_SIZE is set, a smaller download is treated as a failure and retried
#     (guards against truncated/placeholder responses).
#   * If EXTRACT_DIR is set, the archive is extracted into it, stripping a single
#     leading directory (mirroring ExternalProject's own extraction) so callers
#     that override DOWNLOAD_COMMAND still get a ready-to-build source tree.
#
# Examples: 
#  1) From another Cmake file:
#  wire it into an ExternalProject_Add as a custom DOWNLOAD_COMMAND
# (this is the intended usage; ${HELIO_CMAKE_DIR} points at this script's dir):
#
#   ExternalProject_Add(mimalloc2_project
#     DOWNLOAD_COMMAND ${CMAKE_COMMAND}
#         -DURL=https://github.com/microsoft/mimalloc/archive/refs/tags/v2.2.4.tar.gz
#         -DDEST=${THIRD_PARTY_DIR}/mimalloc2-src-archive
#         -DEXTRACT_DIR=${THIRD_PARTY_DIR}/mimalloc2
#         -DEXPECTED_HASH=754a98de5e2912fddbeaf24830f982b4540992f1bab4a0a8796ee118e0752bda
#         -P ${HELIO_CMAKE_DIR}/download_retry.cmake
#      .. <rest of ExternalProject_Add args> ..)
#
# 2) From a shell (assuming in the same folder where this script resides):
#   cmake -DURL=https://github.com/microsoft/mimalloc/archive/refs/tags/v2.2.4.tar.gz \
#         -DDEST=/tmp/mimalloc.tar.gz \
#         -DEXTRACT_DIR=/tmp/mimalloc-src \
#         -DEXPECTED_HASH=754a98de5e2912fddbeaf24830f982b4540992f1bab4a0a8796ee118e0752bda \
#         -P download_retry.cmake

cmake_minimum_required(VERSION 3.10)

# Prefix used in all log/error messages emitted by this script.
set(MODULE_NAME "download_retry")

# Check input
if(NOT DEFINED URL OR URL STREQUAL "")
  message(FATAL_ERROR "${MODULE_NAME}: URL must be provided")
endif()
if(NOT DEFINED DEST OR DEST STREQUAL "")
  message(FATAL_ERROR "${MODULE_NAME}: DEST must be provided")
endif()
if(NOT DEFINED RETRIES OR RETRIES LESS 1)
  set(RETRIES 10)
endif()
if(NOT DEFINED BACKOFF OR BACKOFF STREQUAL "")
  set(BACKOFF 5)
endif()
if(NOT DEFINED BACKOFF_MAX OR BACKOFF_MAX STREQUAL "")
  set(BACKOFF_MAX 60)
endif()

# Ordered list of URLs tried on every attempt (primary first, then fallback).
set(_urls "${URL}")
if(DEFINED URL_FALLBACK AND NOT URL_FALLBACK STREQUAL "")
  list(APPEND _urls "${URL_FALLBACK}")
endif()

# Optional SHA256 verification. We verify manually (rather than via
# file(DOWNLOAD EXPECTED_HASH)) because file(DOWNLOAD) aborts the whole script
# on a hash mismatch; verifying ourselves lets a corrupt/partial download be
# retried like any other failure.
set(_expected_hash "")
if(DEFINED EXPECTED_HASH AND NOT EXPECTED_HASH STREQUAL "")
  string(TOLOWER "${EXPECTED_HASH}" _expected_hash)
  # Accept either a raw hex digest or an "SHA256=<hex>" form (as in URL_HASH).
  if(_expected_hash MATCHES "^sha256=(.*)$")
    set(_expected_hash "${CMAKE_MATCH_1}")
  endif()
endif()

# Start downloading all URLs with retries
set(_downloaded FALSE)
set(_sleep ${BACKOFF})
foreach(_attempt RANGE 1 ${RETRIES})
  foreach(_url IN LISTS _urls)
    message(STATUS "${MODULE_NAME}: attempt ${_attempt}/${RETRIES} from ${_url}")
    file(DOWNLOAD "${_url}" "${DEST}"
         STATUS _status
         LOG _log
         INACTIVITY_TIMEOUT 60
         TIMEOUT 600)
    list(GET _status 0 _code)
    list(GET _status 1 _msg)

    if(_code EQUAL 0 AND DEFINED MIN_SIZE AND NOT MIN_SIZE STREQUAL "")
      file(SIZE "${DEST}" _size)
      if(_size LESS MIN_SIZE)
        set(_code 99)
        set(_msg "downloaded file too small (${_size} bytes < ${MIN_SIZE})")
      endif()
    endif()

    if(_code EQUAL 0 AND NOT _expected_hash STREQUAL "")
      file(SHA256 "${DEST}" _actual_hash)
      if(NOT _actual_hash STREQUAL _expected_hash)
        set(_code 98)
        set(_msg "SHA256 mismatch (expected ${_expected_hash}, got ${_actual_hash})")
      endif()
    endif()

    if(_code EQUAL 0)
      set(_downloaded TRUE)
      break()
    endif()

    message(WARNING "${MODULE_NAME}: attempt ${_attempt} from ${_url} failed (code ${_code}: ${_msg})")
    file(REMOVE "${DEST}")
  endforeach()

  if(_downloaded)
    break()
  endif()

  if(_attempt LESS RETRIES)
    message(STATUS "${MODULE_NAME}: sleeping ${_sleep}s before next attempt")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep "${_sleep}")
    # Exponential backoff, capped at BACKOFF_MAX.
    math(EXPR _sleep "${_sleep} * 2")
    if(_sleep GREATER BACKOFF_MAX)
      set(_sleep ${BACKOFF_MAX})
    endif()
  endif()
endforeach()

if(NOT _downloaded)
  message(FATAL_ERROR "${MODULE_NAME}: download failed after ${RETRIES} attempts: ${URL}")
endif()

message(STATUS "${MODULE_NAME}: downloaded ${DEST}")

if(NOT DEFINED EXTRACT_DIR OR EXTRACT_DIR STREQUAL "")
  return()
endif()

# Extraction
# mirrors CMake's ExternalProject extractfile.cmake logic
get_filename_component(_dest_abs "${DEST}" ABSOLUTE)
get_filename_component(_extract_abs "${EXTRACT_DIR}" ABSOLUTE)
get_filename_component(_parent_dir "${_extract_abs}/.." ABSOLUTE)

# Extract into a unique sibling temp dir, then move into place. The name hashes
# the source/target paths (so parallel extractions of different deps never
# collide) plus a random suffix, avoiding a shared incrementing-counter race.
string(SHA256 _uniq "${_dest_abs}:${_extract_abs}")
string(SUBSTRING "${_uniq}" 0 12 _uniq)
string(RANDOM LENGTH 8 _rand)
set(_tmp_dir "${_parent_dir}/df-dl-ex-${_uniq}-${_rand}")
while(EXISTS "${_tmp_dir}")
  string(RANDOM LENGTH 8 _rand)
  set(_tmp_dir "${_parent_dir}/df-dl-ex-${_uniq}-${_rand}")
endwhile()
file(MAKE_DIRECTORY "${_tmp_dir}")

message(STATUS "${MODULE_NAME}: extracting ${_dest_abs} -> ${_extract_abs}")
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${_dest_abs}"
                WORKING_DIRECTORY "${_tmp_dir}"
                RESULT_VARIABLE _extract_rv)
if(NOT _extract_rv EQUAL 0)
  file(REMOVE_RECURSE "${_tmp_dir}")
  message(FATAL_ERROR "${MODULE_NAME}: extraction of ${_dest_abs} failed")
endif()

# If the archive contains exactly one top-level directory, strip it so the
# source lands directly in EXTRACT_DIR (as ExternalProject would do).
file(GLOB _contents "${_tmp_dir}/*")
list(REMOVE_ITEM _contents "${_tmp_dir}/.DS_Store")
list(LENGTH _contents _n)
if(NOT _n EQUAL 1 OR NOT IS_DIRECTORY "${_contents}")
  set(_contents "${_tmp_dir}")
endif()
get_filename_component(_contents "${_contents}" ABSOLUTE)

file(REMOVE_RECURSE "${_extract_abs}")
file(RENAME "${_contents}" "${_extract_abs}")
file(REMOVE_RECURSE "${_tmp_dir}")

message(STATUS "${MODULE_NAME}: extraction done")
