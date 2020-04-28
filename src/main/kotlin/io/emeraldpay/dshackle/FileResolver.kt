/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle

import java.io.File

class FileResolver(
        private val baseDir: File
) {

    companion object {
        fun isAccessible(file: File): Boolean {
            return file.exists() && file.isFile && file.canRead()
        }
    }

    fun resolve(path: String): File {
        val direct = File(path)
        if (direct.isAbsolute) {
            return direct
        }
        return File(baseDir, path)
    }

}