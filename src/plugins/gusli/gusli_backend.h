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
#include "backend/backend_engine.h"
#include "gusli_client_api.hpp"

static inline nixl_mem_list_t
__getSupportedGusliMems (void) {
    return {BLK_SEG, DRAM_SEG};
}

class nixlGusliEngine : public nixlBackendEngine {
public:
    nixlGusliEngine (const nixlBackendInitParams *init_params);
    ~nixlGusliEngine();
    bool
    supportsNotif (void) const override {
        return false;
    }
    bool
    supportsRemote (void) const override {
        return false;
    }
    bool
    supportsLocal (void) const override {
        return true;
    }
    bool
    supportsProgTh (void) const override {
        return false;
    }
    nixl_mem_list_t
    getSupportedMems (void) const override {
        return __getSupportedGusliMems();
    }
    nixl_status_t
    connect (const std::string &remote_agent) override {
        return NIXL_SUCCESS;
    }
    nixl_status_t
    disconnect (const std::string &remote_agent) override {
        return NIXL_SUCCESS;
    }
    nixl_status_t
    loadLocalMD (nixlBackendMD *in, nixlBackendMD *&out) override {
        out = in;
        return NIXL_SUCCESS;
    }
    nixl_status_t
    unloadMD (nixlBackendMD *input) override {
        return NIXL_SUCCESS;
    }
    [[nodiscard]] nixl_status_t
    registerMem (const nixlBlobDesc &mem, const nixl_mem_t &nixl_mem, nixlBackendMD *&out) override;
    [[nodiscard]] nixl_status_t
    deregisterMem (nixlBackendMD *out) override;

    [[nodiscard]] nixl_status_t
    prepXfer (const nixl_xfer_op_t &operation,
              const nixl_meta_dlist_t &local,
              const nixl_meta_dlist_t &remote,
              const std::string &remote_agent,
              nixlBackendReqH *&io_handle,
              const nixl_opt_b_args_t *opt_args = nullptr) const override;

    [[nodiscard]] nixl_status_t
    postXfer (const nixl_xfer_op_t &operation,
              const nixl_meta_dlist_t &local,
              const nixl_meta_dlist_t &remote,
              const std::string &remote_agent,
              nixlBackendReqH *&io_handle,
              const nixl_opt_b_args_t *opt_args = nullptr) const override;
    [[nodiscard]] nixl_status_t
    checkXfer (nixlBackendReqH *io_handle) const override;
    [[nodiscard]] nixl_status_t
    releaseReqH (nixlBackendReqH *io_handle) const override;

private:
    gusli::global_clnt_context *lib_;
    struct bdevRefcountT { // No support for open/close so use refcount
        gusli::bdev_info bi;
        int ref_count;
    };
    std::unordered_map<uint64_t, bdevRefcountT> bdevs_; // Hash of open block devices
    [[nodiscard]] nixl_status_t
    bdevOpen (uint64_t devId); // open()/close() called implicitly by *registerMem()
    [[nodiscard]] nixl_status_t
    bdevClose (uint64_t devId);
    [[nodiscard]] int32_t
    getGidOfBDev (uint64_t devId) const {
        const bdevRefcountT &v = bdevs_.find (devId)->second; // Already open
        return v.bi.bdev_descriptor;
    }
};
#endif
