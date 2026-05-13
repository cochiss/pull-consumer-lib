package com.smg.pull.lib

data class MegValidationResult(
    val valid: Boolean,
    val message: String? = null
) {
    companion object {
        @JvmStatic
        fun ok(): MegValidationResult = MegValidationResult(valid = true, message = null)

        @JvmStatic
        fun invalid(message: String): MegValidationResult = MegValidationResult(valid = false, message = message)
    }
}
