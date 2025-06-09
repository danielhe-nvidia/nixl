/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __GUSLI_BACKEND_H
#define __GUSLI_BACKEND_H

#include <nixl.h>
#include <nixl_types.h>
#include "common/nixl_log.h"
#include <unistd.h>
#include "backend/backend_engine.h"
#include "gusli_client_api.hpp"
#include <absl/strings/str_format.h>

#define GUSLI_LOG_ERR(format, ...) do { \
	NIXL_ERROR << absl::StrFormat("GUSLI %s() %s[%d]" format, __PRETTY_FUNCTION__, __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)

#define GUSLI_LOG_RETURN(rv, format, ...) do { \
	GUSLI_LOG_ERR("error=%d, " format, (int)rv, ##__VA_ARGS__); \
	return error_code; \
} while (0)

class nixlGusliBackendReqH : public nixlBackendReqH {
	static void completion_cb(nixlGusliBackendReqH *c) {
		NIXL_TRACE << absl::StrFormat("GUSLI IO[op=%c] o[%p]_done, rv=%d\n", c->io.params.op, c, c->io.get_error());
	}
 public:
	gusli::io_request io;
	nixlGusliBackendReqH(const nixl_xfer_op_t _op) {
		io.params.set_completion(this, completion_cb);
		io.params.op = (_op == NIXL_WRITE) ? gusli::G_WRITE : gusli::G_READ;
	}
	~nixlGusliBackendReqH() {}
	void exec(void op) {
		const int n_bytes = (int)io.params.buf_size();
		NIXL_TRACE << absl::StrFormat("GUSLI IO[op=%c] o[%p]start, n_ranges=%u\n", c->io.params.op, this, io.params.num_ranges());
		io.submit_io();
	}
};

class nixlGusliEngine : public nixlBackendEngine {
 private:
	gusli::global_clnt_context* lib;
 public:
	nixlGusliEngine(const nixlBackendInitParams* init_params);
	~nixlGusliEngine();
	bool supportsNotif(void) const { return false; }
	bool supportsRemote(void) const { return false; }
	bool supportsLocal(void) const { return true; }
	bool supportsProgTh(void) const { return false;	}
	nixl_mem_list_t getSupportedMems(void) const {
		nixl_mem_list_t mems;
		mems.push_back(BLK_SEG);		GUSLITODO
		return mems;
	}
	nixl_status_t connect(   const std::string &remote_agent) { return NIXL_SUCCESS; }
	nixl_status_t disconnect(const std::string &remote_agent) { return NIXL_SUCCESS; }
	nixl_status_t loadLocalMD(nixlBackendMD* input, nixlBackendMD* &output) {
		output = input;
		return NIXL_SUCCESS;
	}
	nixl_status_t unloadMD(nixlBackendMD* input) { return NIXL_SUCCESS; }
	nixl_status_t registerMem(const nixlBlobDesc &mem,
								const nixl_mem_t &nixl_mem,
								nixlBackendMD* &out);
	nixl_status_t deregisterMem (nixlBackendMD *meta);

	nixl_status_t prepXfer( const nixl_xfer_op_t &operation,
							const nixl_meta_dlist_t &local,
							const nixl_meta_dlist_t &remote,
							const std::string &remote_agent,
							nixlBackendReqH* &handle,
							const nixl_opt_b_args_t* opt_args=nullptr) const;

	nixl_status_t postXfer( const nixl_xfer_op_t &operation,
							const nixl_meta_dlist_t &local,
							const nixl_meta_dlist_t &remote,
							const std::string &remote_agent,
							nixlBackendReqH* &handle,
							const nixl_opt_b_args_t* opt_args=nullptr) const;
	nixl_status_t checkXfer(  nixlBackendReqH* handle) const;
	nixl_status_t releaseReqH(nixlBackendReqH* handle) const;
};
#endif
