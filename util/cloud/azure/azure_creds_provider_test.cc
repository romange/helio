// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include <gmock/gmock.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "base/gtest.h"
#include "util/cloud/azure/creds_provider.h"

namespace util::cloud::azure {
namespace {

class ScopedEnv {
 public:
  explicit ScopedEnv(const char* name) : name_(name) {
    if (const char* value = getenv(name); value) {
      original_ = value;
    }
    unsetenv(name);
  }

  ~ScopedEnv() {
    if (original_) {
      setenv(name_, original_->c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }

 private:
  const char* name_;
  std::optional<std::string> original_;
};

}  // namespace

class AzureCredentialsTest : public testing::Test {
 protected:
  ScopedEnv connection_string_{"AZURE_STORAGE_CONNECTION_STRING"};
  ScopedEnv account_{"AZURE_STORAGE_ACCOUNT"};
  ScopedEnv endpoint_{"AZURE_STORAGE_BLOB_ENDPOINT"};
  ScopedEnv account_url_{"AZURE_STORAGE_ACCOUNT_URL"};
  ScopedEnv sas_{"AZURE_STORAGE_SAS_TOKEN"};
  ScopedEnv key_{"AZURE_STORAGE_KEY"};
};

TEST_F(AzureCredentialsTest, UsesUriAccountInsteadOfEnvironmentAccount) {
  setenv("AZURE_STORAGE_KEY", "test-key", 1);
  setenv("AZURE_STORAGE_ACCOUNT", "otheraccount", 1);

  Credentials provider{"snapshotaccount"};
  EXPECT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "snapshotaccount");
  EXPECT_EQ(provider.ServiceEndpoint(), "snapshotaccount.blob.core.windows.net");
}

TEST_F(AzureCredentialsTest, ConnectionStringTakesPrecedenceOverEnvironmentCredentials) {
  setenv("AZURE_STORAGE_CONNECTION_STRING",
         " AccountName = connectionaccount ; AccountKey = connection-key ; "
         "EndpointSuffix = private.example.test ",
         1);
  setenv("AZURE_STORAGE_ACCOUNT", "environmentaccount", 1);
  setenv("AZURE_STORAGE_KEY", "environment-key", 1);

  Credentials provider;
  EXPECT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "connectionaccount");
  EXPECT_EQ(provider.account_key(), "connection-key");
  EXPECT_EQ(provider.ServiceEndpoint(), "connectionaccount.blob.private.example.test");
  EXPECT_TRUE(provider.IsHttps());
  EXPECT_TRUE(provider.GetPathPrefix().empty());
}

TEST_F(AzureCredentialsTest, ConnectionStringSupportsHttpPathStyleEndpoint) {
  setenv("AZURE_STORAGE_CONNECTION_STRING",
         "AccountName=devstoreaccount1;AccountKey=test-key;"
         "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1/",
         1);

  Credentials provider;
  ASSERT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "devstoreaccount1");
  EXPECT_EQ(provider.account_key(), "test-key");
  EXPECT_EQ(provider.ServiceEndpoint(), "127.0.0.1:10000");
  EXPECT_FALSE(provider.IsHttps());
  EXPECT_EQ(provider.GetPathPrefix(), "/devstoreaccount1");
}

TEST_F(AzureCredentialsTest, BlobEndpointTakesPrecedenceOverAccountUrl) {
  setenv("AZURE_STORAGE_ACCOUNT", "environmentaccount", 1);
  setenv("AZURE_STORAGE_KEY", "environment-key", 1);
  setenv("AZURE_STORAGE_BLOB_ENDPOINT", "https://blob.example.test:8443/blob-path/", 1);
  setenv("AZURE_STORAGE_ACCOUNT_URL", "https://account-url.example.test/account-path", 1);

  Credentials provider;
  ASSERT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "environmentaccount");
  EXPECT_EQ(provider.account_key(), "environment-key");
  EXPECT_EQ(provider.ServiceEndpoint(), "blob.example.test:8443");
  EXPECT_TRUE(provider.IsHttps());
  EXPECT_EQ(provider.GetPathPrefix(), "/blob-path");
}

TEST_F(AzureCredentialsTest, AccountUrlIsUsedWhenBlobEndpointIsUnset) {
  setenv("AZURE_STORAGE_ACCOUNT", "environmentaccount", 1);
  setenv("AZURE_STORAGE_KEY", "environment-key", 1);
  setenv("AZURE_STORAGE_ACCOUNT_URL", "https://account-url.example.test/account-path", 1);

  Credentials provider;
  ASSERT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "environmentaccount");
  EXPECT_EQ(provider.account_key(), "environment-key");
  EXPECT_EQ(provider.ServiceEndpoint(), "account-url.example.test");
  EXPECT_TRUE(provider.IsHttps());
  EXPECT_EQ(provider.GetPathPrefix(), "/account-path");
}

TEST_F(AzureCredentialsTest, SasTokenIsAppendedUnlessRequestAlreadyHasSignature) {
  setenv("AZURE_STORAGE_ACCOUNT", "sasaccount", 1);
  setenv("AZURE_STORAGE_SAS_TOKEN", " ?sv=2025-01-05&sig=token-signature ", 1);

  Credentials provider;
  ASSERT_FALSE(provider.Init(1));
  EXPECT_EQ(provider.account_name(), "sasaccount");
  EXPECT_TRUE(provider.account_key().empty());

  detail::EmptyRequestImpl request{boost::beast::http::verb::get, "/container/blob?comp=list"};
  provider.Sign(&request);
  EXPECT_EQ(request.GetTarget(), "/container/blob?comp=list&sv=2025-01-05&sig=token-signature");
  auto has_header = [](std::string_view name) {
    return testing::Truly([name](const auto& header) { return header.name_string() == name; });
  };
  EXPECT_THAT(request.GetHeaders(), testing::Contains(has_header("x-ms-date")));
  EXPECT_THAT(request.GetHeaders(), testing::Contains(has_header("x-ms-version")));
  EXPECT_THAT(request.GetHeaders(), testing::Not(testing::Contains(has_header("Authorization"))));

  detail::EmptyRequestImpl signed_request{boost::beast::http::verb::get,
                                          "/container/blob?sig=existing-signature"};
  provider.Sign(&signed_request);
  EXPECT_EQ(signed_request.GetTarget(), "/container/blob?sig=existing-signature");
}

}  // namespace util::cloud::azure
