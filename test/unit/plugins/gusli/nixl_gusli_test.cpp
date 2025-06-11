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

class gtest {		// Gusli tester class
	const size_t page_size = sysconf(_SC_PAGESIZE);
	static constexpr const int default_num_transfers = 1024;
	static constexpr const size_t default_transfer_size = 1 * 512 * 1024; // 512KB
	static constexpr const size_t kb_size = (1 << 10), mb_size = (1 << 20), gb_size = (1 << 30);
	static constexpr const int line_width = 60;
	static constexpr const std::string line_str(line_width, '=');
	static constexpr const char default_test_files_dir_path[] = "tmp/testfiles";
	static constexpr const char* agent_name = "GUSLITester";

	static constexpr double us_to_s(double us) { return us / 1000000.0; }
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
		return absl::StrFormat("PHASE %d: %s", phase_num++, title);
	}

	void print_segment_title(const std::string& title) {
		std::cout << std::endl << line_str << std::endl;
		std::cout << center_str(title) << std::endl;
		std::cout << line_str << std::endl;
	}
}

int main(int argc, char *argv[]) {
	std::cout << "NIXL GUSLI Plugin Test" << std::endl;
	int                opt;
	int                num_transfers = default_num_transfers;
	size_t             transfer_size = default_transfer_size;
	std::string        test_files_dir_path = default_test_files_dir_path;
	long               page_size = sysconf(_SC_PAGESIZE);

    while ((opt = getopt(argc, argv, "n:s:d:DUh")) != -1) {
        switch (opt) {
            case 'n': num_transfers = std::stoi(optarg); break;
            case 's': transfer_size = std::stoull(optarg); break;
            case 'd': test_files_dir_path = optarg; break;
            case 'h':
            default:
                std::cout << absl::StrFormat("Usage: %s [-n num_transfers] [-s transfer_size] [-d test_files_dir_path] [-D] [-U]", argv[0]) << std::endl;
                std::cout << absl::StrFormat("  -n num_transfers      Number of transfers (default: %d)", default_num_transfers) << std::endl;
                std::cout << absl::StrFormat("  -s transfer_size      Size of each transfer in bytes (default: %zu)", default_transfer_size) << std::endl;
                std::cout << absl::StrFormat("  -d test_files_dir_path Directory for test files, strongly recommended to use nvme device (default: %s)", default_test_files_dir_path) << std::endl;
                std::cout << absl::StrFormat("  -D                    Use O_DIRECT for file I/O") << std::endl;
                std::cout << absl::StrFormat("  -h                    Show this help message") << std::endl;
                return (opt == 'h') ? 0 : 1;
        }
    }

	if (page_size <= 0) {
		std::cerr << "Error: Invalid page size returned by sysconf" << std::endl;
		return 1;
	}
	if (num_transfers % 4)					// Make num_transfers aligned to 4
		num_transfers = ((num_transfers + 3) / 4) * 4;
	if ((transfer_size % page_size) != 0) 	// align transfer size to page size
		transfer_size = ((transfer_size + page_size - 1) / page_size) * page_size;

	// Convert directory path to absolute path using std::filesystem
	std::filesystem::path path_obj(test_files_dir_path);
	std::filesystem::create_directories(path_obj);
	std::string abs_path = std::filesystem::absolute(path_obj).string();

	// Initialize NIXL components first
	nixlAgent agent(agent_name, nixlAgentConfig(true));

	// Set up backend parameters for gusli::global_clnt_context::init_params
	nixl_b_params_t params;
	params["log"] = "????";		// GUSLITODO
	params["client_name"] = agent_name;
	params["config_file"] = "# version=1, bdevs: UUID, type, attach_op, direct, path, security_cookie\n"
		"050e8400 f W N ./store.bin  sec=0x3\n"		// Local file in non direct mode
		"2b3f28dc K X D /dev/nvme0n1 sec=0x7\n";	// NVME in direct mode
	params["max_num_simultaneous_requests"] = std::to_string(num_transfers);

	// Print test configuration information
	print_segment_title("NIXL STORAGE TEST STARTING (GUSLI PLUGIN)");
	std::cout << absl::StrFormat("Configuration:\n");
	std::cout << absl::StrFormat("- Number of transfers: %d\n", num_transfers);
	std::cout << absl::StrFormat("- Transfer size: %zu[B]\n", transfer_size);
	std::cout << absl::StrFormat("- Total data: %.2f[GB]\n", (float(transfer_size) * num_transfers) / gb_size);
	std::cout << absl::StrFormat("- Directory: %s\n", abs_path);
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
	//const nixl_status_t status = agent.makeConnection(const std::string &remote_agent, const nixl_opt_args_t* extra_params); GUSLITODO
	int bdev_descriptor = 555555; GUSLITODO
    try {
		print_segment_title(phase_title("Allocating and initializing buffers"));
        // Create descriptor lists
        nixl_xfer_dlist_t bdev_io_src(DRAM_SEG), bdev_io_dst(BLK_SEG);
        std::unique_ptr<nixlBlobDesc[]> dram_buf(new nixlBlobDesc[num_transfers]);
        std::unique_ptr<nixlBlobDesc[]> bdev_buf(new nixlBlobDesc[num_transfers]);

		// Control variables
		nixlTime::us_t total_time(0);
		double total_data_gb(0);

		// Allocate and initialize DRAM buffer
		const size_t n_total_mapped_bytes = num_transfers * transfer_size;
		void* ptr;
		if (posix_memalign(&ptr, page_size, n_total_mapped_bytes) != 0) {
			std::cerr << "DRAM allocation failed" << std::endl;
			return 1;
		}
		test_pattern_do(ptr, n_total_mapped_bytes, "fill");
		for (int i = 0; i < num_transfers; ++i) {
			dram_buf[i].len = transfer_size;
			dram_buf[i].addr = (uintptr_t)((u_int64_t)ptr + (i*transfer_size));
			dram_buf[i].devId = 0;
			bdev_io_src.addDesc(dram_buf[i]);

			bdev_buf[i].len = transfer_size;
			bdev_buf[i].addr = (1<<20) + (i*transfer_size);	// Offset of 1[MB] block-device
			bdev_buf[i].devId = bdev_descriptor;
			bdev_io_dst.addDesc(bdev_buf[i]);
			printProgress(float(i + 1) / num_transfers);
		}

		print_segment_title(phase_title("Registering memory with NIXL"));
		nixl_reg_dlist_t dram_reg(DRAM_SEG), bdev_reg(BLK_SEG);
		{	// Register the large buffer as 2 halfs, just for testing > 1 buf
			nixlBlobDesc d;
			d.len = n_total_mapped_bytes/2;
			d.devId = 0;
			d.addr = (uintptr_t)((u_int64_t)ptr + 0*d.len); dram_reg.addDesc(d);
			d.addr = (uintptr_t)((u_int64_t)ptr + 1*d.len); dram_reg.addDesc(d);

			d.len = n_total_mapped_bytes/4;	// GUSLITODO Why is it needed?
			d.devId = bdev_descriptor;
			d.addr = (1<<20) + 0*d.len; bdev_reg.addDesc(d);
			d.addr = (1<<20) + 1*d.len; bdev_reg.addDesc(d);
			d.addr = (1<<20) + 2*d.len; bdev_reg.addDesc(d);
			d.addr = (1<<20) + 3*d.len; bdev_reg.addDesc(d);

			i = 0;
			enum nixl_status_t rv;
			rv = agent.registerMem(dram_reg);
			if (rv != NIXL_SUCCESS) {
				std::cerr << "Failed reg:" << nixlEnumStrings::memTypeStr(dramg_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(float(++i) / 2);
			rv = agent.registerMem(bdev_reg);
			if (rv != NIXL_SUCCESS) {
				std::cerr << "Failed reg:" << nixlEnumStrings::memTypeStr(bdev_reg.getType()) << ", rv=" << nixlEnumStrings::statusStr(rv) << std::endl;
				return 1;
			}
			printProgress(float(i + 1) / 2);
		}

		enum nixl_xfer_op_t io_phases = {NIXL_WRITE, NIXL_READ};	// First write - then read
		for (int i = 0; i < 2; i++ ) {
			const std::string io_t_str = nixlEnumStrings::xferOpStr(io_phases[i]);
			print_segment_title(phase_title(io_t_str + " Test"));
			nixlXferReqH* treq = nullptr;
			nixl_status_t status = agent.createXferReq(io_phases[i], bdev_io_src, bdev_io_dst, agent_name, treq);
			if (status != NIXL_SUCCESS) {
				std::cerr << "Failed to create transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
				return 1;
			}
			const nixlTime::us_t time_start = nixlTime::getUs();
			status = agent.postXferReq(treq);
			if (status < 0) {
				std::cerr << "Failed to post transfer request - status: " << nixlEnumStrings::statusStr(status) << std::endl;
				agent.releaseXferReq(treq);
				return 1;
			}
			do { // Wait for transfer to complete
				status = agent.getXferStatus(treq);
				if (status < 0) {
					std::cerr << "Error during transfer - status: " << nixlEnumStrings::statusStr(status) << std::endl;
					agent.releaseXferReq(treq);
					return 1;
				}
			} while (status == NIXL_IN_PROG);

			const nixlTime::us_t time_end = nixlTime::getUs();
			const nixlTime::us_t time_duration = time_end - time_start;
			const double data_gb = (float(transfer_size) * num_transfers) / (gb_size);
			const double seconds = us_to_s(time_duration);
			const double gbps = data_gb / seconds;
			std::cout << io_t_str << ": completed with status: " << nixlEnumStrings::statusStr(status) << std::endl;
			std::cout << "- Time: " << format_duration(time_duration) << std::endl;
			std::cout << "- Data: " << std::fixed << std::setprecision(2) << data_gb << "[GB]" << std::endl;
			std::cout << "- Speed: " << gbps << "[GB/s]" << std::endl;
			total_time += time_duration;
			total_data_gb += data_gb;

			if (io_phases[i] == NIXL_WRITE) {		// Clear buffers before read
				print_segment_title(phase_title("Clearing DRAM buffers"));
				test_pattern_do(ptr, n_total_mapped_bytes, "clear");
			}

			print_segment_title("Freeing resources");
			agent.releaseXferReq(treq); }
		}
		print_segment_title(phase_title("Validating read data"));
		test_pattern_do(ptr, n_total_mapped_bytes, "verify");

		print_segment_title(phase_title("Un-Registering memory with NIXL"));
		agent.deregisterMem(bdev_reg);
		agent.deregisterMem(dram_reg);
		free(ptr);

		print_segment_title("TEST SUMMARY");
		std::cout << "Total time: " << format_duration(total_time) << std::endl;
		std::cout << "Total data: " << std::fixed << std::setprecision(2) << total_data_gb << " GB" << std::endl;
		std::cout << line_str << std::endl;

		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Exception during test execution: " << e.what() << std::endl;
		return -1;
	}
}
