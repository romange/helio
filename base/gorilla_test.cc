// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace base {

class GorillaTest : public testing::Test {
 protected:
};

const size_t VECTOR_SIZE = 8;
const size_t MIN_DATA_SIZE_COMPRESSION_TRESHOLD = 2 * VECTOR_SIZE;
int PROBABILITY_OF_REPEATING = 80;  // percent

inline int getBytesLengthOfOffsets(int offsetsCount) {
	// * 3 = bits per one offset
	// + 7 = round up to bytes
	// >>  = get number of bytes
	return (offsetsCount * 3 + 7) >> 3;
}

inline int floor8(int x) {
	return x & ~7;
}

void fillStart(std::vector<double>& data, std::vector<char>& output, size_t blockSize) {
	double* outAsLong = reinterpret_cast<double*>(output.data());
	for (size_t i = 0; i < VECTOR_SIZE; i++) {
		outAsLong[i] = data[blockSize * i];
	}
}

size_t DoubleCompress(std::vector<double>& data, std::vector<char>& output) {
  using T = double;

  CHECK_GT(data.size(), MIN_DATA_SIZE_COMPRESSION_TRESHOLD);


	// skip first 8 init values
	size_t outputIndex = sizeof(int64_t) * VECTOR_SIZE;
	// "middle-out" data block size
	size_t blockSize = data.size() / VECTOR_SIZE;
	// just copy init reference values
	fillStart(data, output, blockSize);

	// main compression loop
	for (size_t i = 1; i < blockSize; i += 1) {
		uint8_t sameMask = 0;  // bit mask if current value is same as previous one
		int maxLength = 0;     // max (within 8 values) length of compressed value in bytes

		// offset within xored value is number of bytes from right where starts non-zero bits after
		// xored with the previous value (rounded down to bytes).
		// These values are 3bits and are stored (scattered) in 3 bottom bytes of this int
		uint32_t compressedOffsets = 0;

		// store xored and accordingly alligned (shifted) values in temp array
		// temp array allows perform compression logic wihout loop dependency
		uint64_t xoredShifted[8] = {0, 0, 0, 0, 0, 0, 0, 0};

		// if set, store index for next value will be shifted and stored data would not be overriten
		int dataStoreFlags[8] = {0, 0, 0, 0, 0, 0, 0, 0};

		uint32_t offsetsShift = 3;  // skip 3 bits for max length
		int notSameCount = 0;

		for (size_t j = 0; j < VECTOR_SIZE; j++) {
			// offset within input vector
			size_t offset = blockSize * j + i;
			// previous value - used for xor
			int64_t prev = reinterpret_cast<uint64_t&>(data[offset - 1]);
			int64_t curr = reinterpret_cast<uint64_t&>(data[offset]);

			// xore current value with previous
			int64_t xored = prev xor curr;

			// if data did not change
			if (xored == 0) {
				// is same as previous value
				// just set appropriate bit
				sameMask = sameMask | (1 << j);
				continue;
			}

			// this value will be stored
			dataStoreFlags[j] = 1;

			int leadingZeros = __builtin_clzl(xored);
			int trailingZeros = __builtin_ctzl(xored);

			int rightOffsetBits = floor8(trailingZeros);
			int rightOffsetBytes = trailingZeros >> 3;

			int leadingZerosBytes = leadingZeros >> 3;
			int trailingZerosBytes = trailingZeros >> 3;
			// compute length of non-zero data wihin xored value
			int lengthBytes = 8 - leadingZerosBytes - trailingZerosBytes;

			maxLength = std::max(lengthBytes, maxLength);

			// scatter current offset
			compressedOffsets |= rightOffsetBytes << offsetsShift;
			offsetsShift += 3;
			notSameCount++;

			// write value to tmp array aligned to right
			xoredShifted[j] = xored >> rightOffsetBits;
		}

		output[outputIndex++] = sameMask;

		// do not store max length and offsets if all values are the same
		if (sameMask == 0b11111111) {
			continue;
		}

		int* outAsInts = reinterpret_cast<int*>(&output[outputIndex]);
		// write 4 bytes with allready writed data and scattered offsets
		// -1 becasue we need to store only values 1-8, so 3 bits are enought
		outAsInts[0] = compressedOffsets | (maxLength - 1);

		// skip bytes poluted by offsets, rouded up to bytes
		// yes, we waste here some bits, but it is for the sake of performance
		// +1 because first 3 bits are maxLength
		outputIndex += getBytesLengthOfOffsets(notSameCount + 1);

		// write prepared data
		for (size_t j = 0; j < VECTOR_SIZE; j++) {
			T* outAsLongs = reinterpret_cast<T*>(&output[outputIndex]);

			outAsLongs[0] = reinterpret_cast<T&>(xoredShifted[j]);
			// shift index
			outputIndex += dataStoreFlags[j] * maxLength;
		}
	}

	// write rest of the data without any compression
	for (size_t i = blockSize * VECTOR_SIZE; i < data.size(); i++) {
		T* outAsLongs = reinterpret_cast<T*>(&output[outputIndex]);
		outAsLongs[0] = data[i];
		outputIndex += sizeof(T);
	}

	// write compress algorithm version and datatype constant
	output[outputIndex++] = 0x7E;  //== 0b01111110

	// to avoid access to invalid memory on decompression
	return outputIndex + 6;
}


/**
    Generate mock data.
    Value will be same as previous with probability of PROBABILITY_OF_REPEATING

    For int data type it generates sequence like 0, 1, 2, 3, ...
    For double it generates 0, 0.1, 0.2, 0.3, ...


    @param from
    @param to
    @retun pointer to generated data. Caller must call destructor
*/
std::vector<double> generateSequence(double from, double to) {

	std::vector<double> data(to - from);

	data.push_back(0);
	for (int64_t i = 1; i < (to - from); i++) {
		if (rand() % 100 < PROBABILITY_OF_REPEATING) {
			// make this value same as previous value
			data[i] = data.at(i - 1);
		} else {
				// double sequence growths by 0.1
				data[i] = 0.1 * i;
		}
	}

	return data;
}

TEST_F(GorillaTest, DoubleCompress) {
  std::vector<double> data = generateSequence(100.0, 10000.0);
  size_t inp_sz = data.size() * 8;
  vector<char> outp(inp_sz);
  size_t outp_sz = DoubleCompress(data, outp);

  double ratio = (double) outp_sz / inp_sz;
  LOG(INFO) << "Input size: " << inp_sz << " output size " << outp_sz << " ratio " << ratio
            << " bytes per item: " << (double)outp_sz / data.size();
}

}  // namespace base