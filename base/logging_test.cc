// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "base/logging.h"

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/reflection.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "base/file_log_sink.h"
#include "base/gtest.h"

ABSL_DECLARE_FLAG(bool, alsologtostderr);
ABSL_DECLARE_FLAG(bool, logtostderr);
ABSL_DECLARE_FLAG(std::string, vmodule);

namespace base {
namespace {

class ScopedEnv {
 public:
  explicit ScopedEnv(const char* name) : name_(name) {
    if (const char* value = getenv(name))
      original_ = value;
    CHECK_EQ(unsetenv(name), 0);
  }

  ~ScopedEnv() {
    if (original_) {
      CHECK_EQ(setenv(name_, original_->c_str(), 1), 0);
    } else {
      CHECK_EQ(unsetenv(name_), 0);
    }
  }

 private:
  const char* name_;
  std::optional<std::string> original_;
};

class LoggingTest : public testing::Test {
 protected:
  void SetUp() override {
    absl::SetFlag(&FLAGS_alsologtostderr, false);
    absl::SetFlag(&FLAGS_logtostderr, false);
    absl::SetFlag(&FLAGS_vmodule, "");
    absl::SetStderrThreshold(absl::LogSeverityAtLeast::kError);
  }

  absl::FlagSaver flags_;
  ScopedEnv alsologtostderr_{"GLOG_alsologtostderr"};
  ScopedEnv logtostderr_{"GLOG_logtostderr"};
  ScopedEnv vmodule_{"GLOG_vmodule"};
};

TEST_F(LoggingTest, DefaultMaxLogSize) {
  auto* flag = absl::FindCommandLineFlag("max_log_size");
  ASSERT_NE(flag, nullptr);
  EXPECT_EQ(flag->DefaultValue(), "200");
}

TEST_F(LoggingTest, NoEnvironmentKeepsFlags) {
  absl::SetFlag(&FLAGS_logtostderr, true);
  absl::SetFlag(&FLAGS_vmodule, "logging_test=2");

  InitLoggingFlagsFromEnv();

  EXPECT_TRUE(absl::GetFlag(FLAGS_logtostderr));
  EXPECT_EQ(absl::GetFlag(FLAGS_vmodule), "logging_test=2");
}

TEST_F(LoggingTest, BooleanEnvironmentValues) {
  for (const char* value : {"true", "1", "false", "0"}) {
    ASSERT_EQ(setenv("GLOG_alsologtostderr", value, 1), 0);
    ASSERT_EQ(setenv("GLOG_logtostderr", value, 1), 0);

    InitLoggingFlagsFromEnv();

    bool enabled = std::string(value) == "true" || std::string(value) == "1";
    EXPECT_EQ(absl::GetFlag(FLAGS_alsologtostderr), enabled);
    EXPECT_EQ(absl::GetFlag(FLAGS_logtostderr), enabled);
  }
}

TEST_F(LoggingTest, AlsoLogToStderrKeepsFileLogging) {
  ASSERT_EQ(setenv("GLOG_alsologtostderr", "1", 1), 0);
  InitLoggingFlagsFromEnv();

  FileLogSink sink;
  sink.Init();

  EXPECT_EQ(absl::StderrThreshold(), absl::LogSeverityAtLeast::kInfo);
  EXPECT_FALSE(sink.base_dir().empty());
}

TEST_F(LoggingTest, LogToStderrDisablesFileLogging) {
  ASSERT_EQ(setenv("GLOG_logtostderr", "true", 1), 0);
  InitLoggingFlagsFromEnv();

  FileLogSink sink;
  sink.Init();

  EXPECT_EQ(absl::StderrThreshold(), absl::LogSeverityAtLeast::kInfo);
  EXPECT_TRUE(sink.base_dir().empty());
}

TEST_F(LoggingTest, VModuleEnvironmentUpdatesVerbosity) {
  EXPECT_FALSE(ABSL_VLOG_IS_ON(2));
  ASSERT_EQ(setenv("GLOG_vmodule", "logging_test=2,other_module=1", 1), 0);

  InitLoggingFlagsFromEnv();

  EXPECT_EQ(absl::GetFlag(FLAGS_vmodule), "logging_test=2,other_module=1");
  EXPECT_TRUE(ABSL_VLOG_IS_ON(2));
  EXPECT_FALSE(ABSL_VLOG_IS_ON(3));
}

TEST_F(LoggingTest, CommandLineOverridesEnvironment) {
  ASSERT_EQ(setenv("GLOG_alsologtostderr", "1", 1), 0);
  ASSERT_EQ(setenv("GLOG_logtostderr", "1", 1), 0);
  ASSERT_EQ(setenv("GLOG_vmodule", "logging_test=2", 1), 0);
  InitLoggingFlagsFromEnv();

  char program[] = "logging_test";
  char also_stderr[] = "--noalsologtostderr";
  char stderr_only[] = "--logtostderr=false";
  char vmodule[] = "--vmodule=logging_test=1";
  char* argv[] = {program, also_stderr, stderr_only, vmodule, nullptr};
  absl::ParseCommandLine(4, argv);

  EXPECT_FALSE(absl::GetFlag(FLAGS_alsologtostderr));
  EXPECT_FALSE(absl::GetFlag(FLAGS_logtostderr));
  EXPECT_EQ(absl::GetFlag(FLAGS_vmodule), "logging_test=1");
  EXPECT_TRUE(ABSL_VLOG_IS_ON(1));
  EXPECT_FALSE(ABSL_VLOG_IS_ON(2));

  FileLogSink sink;
  sink.Init();
  EXPECT_EQ(absl::StderrThreshold(), absl::LogSeverityAtLeast::kError);
  EXPECT_FALSE(sink.base_dir().empty());
}

TEST_F(LoggingTest, InvalidBooleanFailsExplicitly) {
  ASSERT_EQ(setenv("GLOG_logtostderr", "invalid", 1), 0);
  EXPECT_EXIT(InitLoggingFlagsFromEnv(), testing::ExitedWithCode(EXIT_FAILURE),
              "Invalid GLOG_logtostderr:");
}

}  // namespace
}  // namespace base
