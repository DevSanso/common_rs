use crate::CommonErrorKind;

pub enum CommonErrorList {
	Critical,
	Etc,
	InvalidApiCall,
	MaxSize,
	NoData,
	NoSupport,
	NotMatchArgs,
	OverFlowMemory,
	ParsingFAil,
}

impl CommonErrorKind for CommonErrorList {
	fn message(&self) -> &'static str {
		match self {
			CommonErrorList::Critical => "critical error, need restart system",
			CommonErrorList::Etc => "etc error",
			CommonErrorList::InvalidApiCall => "invalid api call",
			CommonErrorList::MaxSize => "memory is used Max size",
			CommonErrorList::NoData => "no data",
			CommonErrorList::NoSupport => "not support function",
			CommonErrorList::NotMatchArgs => "args count not matching",
			CommonErrorList::OverFlowMemory => "overflow memory size",
			CommonErrorList::ParsingFAil => "parsing failed",
		}
	}
}
