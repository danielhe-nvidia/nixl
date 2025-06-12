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
#include <filesystem>
#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iomanip>
#include <cassert>
#include <cstring>
#include <string>
#include <absl/strings/str_format.h>
#include "nixl.h"
#include "nixl_params.h"
#include "nixl_descriptors.h"
#include "common/nixl_time.h"
#include <stdexcept>
#include <cstdio>
#include <getopt.h>
//sudo apt-get install apt-transport-https ca-certificates curl gnupg lsb-release
// sudo apt-get install docker; sudo apt install podman-docker
class gtest {		// Gusli tester class
	static constexpr const int default_num_transfers = 1024;
	static constexpr const size_t default_transfer_size = 1 * 512 * 1024; // 512KB
	static constexpr const size_t gb_size = (1 << 30);
	static constexpr const int line_width = 60;
	static constexpr const std::string line_str(line_width, '=');
	static constexpr const char* agent_name = "GUSLITester";

 private:
	int num_transfers;
	size_t transfer_size;
	long page_size;
	void* ptr;					// RAM buffer for ios

	static std::string center_str(const std::string& str) { return std::string((line_width - str.length()) / 2, ' ') + str; }

	bool test_pattern_do(void* buffer, size_t size, const char* action) {
		static constexpr const size_t test_phrase_len = 32;
		char test_phrase[test_phrase_len+1] __attribute__((aligned(sizeof(long))));
		strcpy(test_phrase, "|NIXL bdev 32[b] GUSLI pattern |");
		char* buf = (char*)buffer;
		if (action[0] == 'f') {						// Fill
			for (size_t i; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				memcpy(&buf[i], test_phrase, test_phrase_len);
			}
		} else if (action[0] == 'c') {				// Clear
			memset(buffer, 0, size);
		} else {									// Verify
			for (size_t i; i < size; i += test_phrase_len) {
				*((size_t*)&test_phrase[24]) = i;		// Unique last 64 bits for each 32[b] string
				if (0 != memcmp(&buf[i], test_phrase, test_phrase_len)) {
					std::cerr << "DRAM buffer " << i << " validation failed with error\n";
					return false;
				}
			}
		}
		return true;
	}

	static std::string format_duration(nixlTime::us_t us) {	// Helper function to format duration
		const nixlTime::ms_t ms = us/1000.0;
		if (ms < 1000)
			return absl::StrFormat("%.0f[ms]", ms);
		const double seconds = ms / 1000.0;
		return absl::StrFormat("%.3f[sec]", seconds);
	}

	void printProgress(float progress) {
		static constexpr const int progress_bar_width = (line_width - 2); // -2 for the brackets
		std::cout << "[";
		const int pos = (progress_bar_width * progress);
		for (int i = 0; i < progress_bar_width; ++i) {
			if (i < pos) std::cout << "=";
			else if (i == pos) std::cout << ">";
			else std::cout << " ";
		}
		std::cout << absl::StrFormat("] %.1f%% ", progress * 100.0);
		if (progress >= 1.0) {	// Add completion indicator
			std::cout << "DONE!" << std::endl;
		} else {
			std::cout << "\r";
			std::cout.flush();
		}
	}
	std::string phase_title(const std::string& title) {
		static int phase_num = 1;
		return absl::StrFormat("PHASE %d: %s", phase_num++, title.c_str());
	}

	void print_segment_title(const std::string& title) {
		std::cout << std::endl << line_str << std::endl << center_str(title) << std::endl << line_str << std::endl;
	}

 public:
	gtest(int _num_transfers, size_t _transfer_size) : num_transfers(_num_transfers), transfer_size(_transfer_size), page_size(-1), ptr(nullptr) {
		page_size = sysconf(_SC_PAGESIZE);
		if (page_size <= 0) {
			std::cerr << "Error: Invalid page size returned by sysconf" << std::endl;
			return;
		}
		if (num_transfers % 4)					// Make num_transfers aligned to 4
			num_transfers = ((num_transfers + 3) / 4) * 4;
		if ((transfer_size % page_size) != 0) 	// align transfer size to page size
			transfer_siz
			e = ((transfer_size + page_size - 1) / page_size) * page_size;
	}
	~gtest() {
		if (ptr) free(ptr);
	}

	int run_write_read_verify(void) {
		nixlAgent agent(agent_name, nixlAgentConfig(true));

		// Set up backend parameters for gusli::global_clnt_context::init_params
		#define UUID_LOCAL_FILE "050e8400050e8400"
		#define UUID_NVME__DISK "2b3f28dc2b3f28dc"
		nixl_b_params_t params;
		params["client_name"] = agent_name;
		params["config_file"] = "# version=1, bdevs: UUID-16b, type, attach_op, direct, path, security_cookie\n"
			UUID_LOCAL_FILE " f W N ./store.bin  sec=0x3\n"		// Local file in non direct mode
			UUID_NVME__DISK " K X D /dev/nvme0n1 sec=0x7\n";	// NVME in direct mode
		params["max_num_simultaneous_requests"] = std::to_string(num_transfers);

		// Print test configuration information
		const size_t n_total_mapped_bytes = num_transfers * transfer_size;
		print_segment_title("NIXL STORAGE TEST STARTING (GUSLI PLUGIN)");
		std::cout << absl::StrFormat("Configuration:\n");
		std::cout << absl::StrFormat("- Number of transfers=%d\n", num_transfers);
		std::cout << absl::StrFormat("- Transfer=%zu[B]\n", transfer_size);
		std::cout << absl::StrFormat("- Total data: %.2f[GB]\n", float(n_total_mapped_bytes) / gb_size);
		std::cout << absl::StrFormat("- Backend: %s\n", "GUSLI");
		std::cout << absl::StrFormat("- Direct I/O: %s\n", "enabled");
		std::cout << std::endl << line_str << std::endl;

		// Create GUSLI backend first - before allocating any resources
		nixlBackendH* n_backend = nullptr;
		{
			const nixl_status_t status = agent.createBackend("GUSLI", params, n_backend);
			if (status != NIXL_SUCCESS) {
				std::cerr << std::endl << line_str << std::endl << center_str("ERROR: Backend Creation Failed") << std::endl << line_str << std::endl;
				std::cerr << "Error creating GUSLI backend: " << nixlEnumStrings::statusStr(status) << std::endl;
				std::cerr << std::endl << line_str << std::endl;
				return 1;
			}
		}
		int bdev_descriptor = 555555; GUSLITODO

		print_segment_title(phase_title("Allocating and initializing buffers"));
		nixl_xfer_dlist_t bdev_io_src(DRAM_SEG), bdev_io_dst(BLK_SEG);
		if (posix_memalign(&ptr, page_size, n_total_mapped_bytes) != 0) {
			std::cerr << "DRAM allocation failed" << std::endl;
			return 1;
		}
		test_pattern_do(ptr, n_total_mapped_bytes, "fill");
		for (int i = 0; i < num_transfers; ++i) {
			nixlBlobDesc d;
			d.len = transfer_size;
			d.addr = (uintptr_t)((u_int64_t)ptr + (i*transfer_size));	// Offset in RAM buffer
			d.devId = 0;
			bdev_io_src.addDesc(d);

			d.addr = (1<<20) + (i*transfer_size);	// Offset of 1[MB] block-device + RAM buffer offset
			d.devId = bdev_descriptor;
			bdev_io_dst.addDesc(d);
			printProgress(float(i + 1) / num_transfers);
		}

		print_segment_title(phase_title("Registering memory with NIXL"));
		nixl_reg_dlist_t dram_reg(DRAM_SEG), bdev_reg(BLK_SEG);
		{	// Register the large buffer as 2 halfs, just for testing > 1 buf
			nixlBlobDesc d;
			d.len = n_total_mapped_bytes/2;
			d.devId = 0;
			for (int i = 0; i < 2; i++ )
				d.addr = (uintptr_t)((u_int64_t)ptr + i*d.len); dram_reg.addDesc(d);

			d.len = n_total_mapped_bytes/4;	// GUSLITODO Why is it needed?
			d.devId = bdev_descriptor;
			for (int i = 0; i < 4; i++ )
				d.addr = (1UL << 20) + i*d.len; bdev_reg.addDesc(d);

			enum nixl_status_t rv;
			rv = agent.registerMem(dram_reg);
			if (rv != NIXL_SUCCESS) {
				std::cerr << "Failed reg:" << nixlEnumStrings::memTypeStr(dramg_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(0.5f);
			rv = agent.registerMem(bdev_reg);
			if (rv != NIXL_SUCCESS) {
				std::cerr << "Failed reg:" << nixlEnumStrings::memTypeStr(bdev_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(1.0f);
		}

		nixlTime::us_t total_time(0);
		double total_data_gb(0);
		enum nixl_xfer_op_t io_phases = {NIXL_WRITE, NIXL_READ};	// First write - then read
		for (int i = 0; i < 2; i++ ) {
			const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
			print_segment_title(phase_title(io_t_str + " Test"));
			nixlXferReqH* treq = nullptr;
			nixl_status_t status = agent.createXferReq(io_phases[i], bdev_io_src, bdev_io_dst, agent_name, treq);
			if (status != NIXL_SUCCESS) { std::cerr << "Failed to create req, rv=" << nixlEnumStrings::statusStr(status) << std::endl; if (treq) agent.releaseXferReq(treq); return -1; }
			const nixlTime::us_t time_start = nixlTime::getUs();
			status = agent.postXferReq(treq);
			if (status < 0) { std::cerr << "Failed to post req, rv=" << nixlEnumStrings::statusStr(status) << std::endl; if (treq) agent.releaseXferReq(treq); return -1; }
			do { // Wait for transfer to complete
				status = agent.getXferStatus(treq);
				if (status < 0) { std::cerr << "Failed during transfer req, rv=" << nixlEnumStrings::statusStr(status) << std::endl; if (treq) agent.releaseXferReq(treq); return -1; }
			} while (status == NIXL_IN_PROG);

			const nixlTime::us_t time_end = nixlTime::getUs();
			const nixlTime::us_t time_duration = time_end - time_start;
			const double data_gb = (double)n_total_mapped_bytes / (double)gb_size;
			const double seconds = (time_duration / 1000000.0);
			std::cout << io_t_str << " completed with rv=" << nixlEnumStrings::statusStr(status) << std::endl;
			std::cout << "- Time: " << format_duration(time_duration) << std::endl;
			std::cout << "- Data: " << std::fixed << std::setprecision(2) << data_gb << "[GB]" << std::endl;
			std::cout << "- Speed: " << (data_gb / seconds) << "[GB/s]" << std::endl;
			total_time += time_duration;
			total_data_gb += data_gb;

			if (io_phases[i] == NIXL_WRITE) {		// Clear buffers before read
				print_segment_title(phase_title("Clearing DRAM buffers"));
				test_pattern_do(ptr, n_total_mapped_bytes, "clear");
			}
			print_segment_title("Freeing resources");
			agent.releaseXferReq(treq);
		}
		print_segment_title(phase_title("Validating read data"));
		test_pattern_do(ptr, n_total_mapped_bytes, "verify");

		print_segment_title(phase_title("Un-Registering memory with NIXL"));
		agent.deregisterMem(bdev_reg);
		agent.deregisterMem(dram_reg);

		print_segment_title("TEST SUMMARY");
		std::cout << "Total time: " << format_duration(total_time) << std::endl;
		std::cout << "Total data: " << std::fixed << std::setprecision(2) << total_data_gb << "[GB]" << std::endl;
		std::cout << line_str << std::endl;
		return 0;
	}
}

int main(int argc, char *argv[]) {
	std::cout << "NIXL GUSLI Plugin Test" << std::endl;
	int opt, num_transfers = gtest::default_num_transfers;
	size_t transfer_size = gtest::default_transfer_size;

	while ((opt = getopt(argc, argv, "n:s:d:h")) != -1) {
		switch (opt) {
			case 'n': num_transfers = std::stoi( optarg); break;
			case 's': transfer_size = std::stoull(optarg); break;
			case 'h':
			default:
				std::cout << absl::StrFormat("Usage: %s [-n num_transfers] [-s transfer_size] [-h]", argv[0]) << std::endl;
				std::cout << absl::StrFormat("  -n num_transfers      Number of transfers (default: %d)", gtest::default_num_transfers) << std::endl;
				std::cout << absl::StrFormat("  -s transfer_size      Size of each transfer in bytes (default: %zu)", gtest::default_transfer_size) << std::endl;
				std::cout << absl::StrFormat("  -h                    Show this help message") << std::endl;
				return (opt == 'h') ? 0 : 1;
		}
	}

	gtest base_test(num_transfers, transfer_size);
	if (base_test.run_write_read_verify() < 0)
		return -1;
	return 0;
}
