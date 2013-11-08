/*
	Elastic CSS Framework
	Released under the MIT, BSD, and GPL Licenses.
	More information http://www.elasticss.com
	
	Elastic Engine Module
	Provides
		Pixel rounding
		Calculations
		Helpers
		Events
		Configuration

	@author     Fernando Trasviña (@azendal)
	@core team  Sergio de la Garza (@sgarza), Javier Ayala (@javi_ayala)
	@copyright  2009 Elastic CSS framework
	@version    2.0.3
*/
(function($){
	var CStyle = function (element, pseudoElement) {
		if (window.getComputedStyle){
			return window.getComputedStyle(element, pseudoElement);
		}
		else{
			return element.currentStyle;
		}
	};

	var width = function(element){
		var width = CStyle(element).width;
		if(width == 'auto' || width.indexOf('px') < 0){
			return $(element).width();
		}else{
			return parseFloat(width);
		}
	};

	window.Elastic = function Elastic(context){
		var r,ra,i,j,k,l,il,jl,kl,ll,
		econs, econ, econw, econclass, ecols, ecol, ecolclass, eg, egml, egcl, egnl, ecrw, ecw, escol, rp, ig,
		efcs, efcsw, eecs, eecsw, eecw, eecrw, ecs, ecsw, ec, ecclass, eecfw,
		ecolminw, ecolmaxw,
		egreg = /(^|\s+)on\-(\d+)(\s+|$)/,
		esreg = /(^|\s+)span\-(\d+)(\s+|$)/,
		eareg = /(^|\s+)adaptive\-(\d+)\-(\d+)(\s+|$)/;

		eg   = [];
		egcl = egnl = 0;

		econs = $.find('.columns', context);
		for(i = 0, il = econs.length; i < il; i++){
			econ = econs[i];
			econclass = econ.className;
			if(econclass.indexOf('on-') > -1 && egreg.test(econclass)){ egml = Number(RegExp.$2);}
			else{                                                       egml = $.find('> .column, > .container > .column', econ).length; }
			econ  = $.find('> .container', econ)[0] || econ;
			econw = width(econ);
			ecrw  = econw / egml;
			ecw   = Math.round( ecrw );
			
			if(econclass.indexOf('adaptive-') > -1 && eareg.test(econclass)){
				ecolminw = Number(RegExp.$2);
				ecolmaxw = Number(RegExp.$3);
				
				if(ecw > ecolmaxw){
					while(ecw > ecolmaxw){
						egml  = egml + 1;
						ecrw  = econw / egml;
						ecw   = Math.round( ecrw );
					}
				}
				else if(ecw < ecolminw){
					while(ecw < ecolminw){
						egml  = egml - 1;
						ecrw  = econw / egml;
						ecw   = Math.round( ecrw );
					}
				}
			}
			
			ecols = $.find('> .column', econ);
			for(j = 0, jl = ecols.length; j < jl; j++){
				efcs  = []; eecs  = []; ecs   = [];
				rp    = ig = efcsw = ecsw = 0;
				ecol  = ecols[j];
				ecolclass = ecol.className;
				escol = 1;
				if(ecolclass.indexOf('span-') > -1 && esreg.test(ecolclass)){ escol = Number(RegExp.$2); }
				ecol.escol = escol = ( (escol <= egml) ? escol : egml);
				egnl += escol;

				if(egnl == egml || j == (jl - 1) || ecolclass.indexOf('final') > -1){ eg.push(ecol); egcl = 0;     rp = 1; }
				else if(egnl <  egml){                                                eg.push(ecol); egcl = egnl; }
				else if(egnl >  egml){                                                               egcl = escol; rp = 1; ig = 1; }

				if(rp){
					for(k = 0, kl = eg.length; k < kl; k++){
						ec      = eg[k];
						ecclass = ec.className;
						if(     ecclass.indexOf('fixed')   > -1){ efcs.push(ec); efcsw += width(ec); }
						else if(ecclass.indexOf('elastic') > -1){ eecs.push(ec); }
						else                                    {
							ecs.push(ec); 
							if(ec == eg[kl - 1]){
								ec.style.width = Math.round(ecrw * ec.escol) + 'px';	
							}
							else{
								ec.style.width = Math.round(ecw * ec.escol) + 'px';	
							}
							ecsw = ecsw + width(ec);
						}
					}
					ll = eecs.length;
					if(ll > 0){
						eecfw = econw - ( ecsw + efcsw );
						if(eecfw <= 0){ continue; }
						eecrw = eecfw / ll;
						eecw  = Math.round( eecrw );
						eecsw = eecw * ll;
						
						for(l=0; l<ll; l++){ eecs[l].style.width = eecw + 'px'; }
						
						if(eecsw > eecfw){
							ra = eecsw - eecfw + 1;
							for(r = 1; r < ra; r++){
								eecs[eecs.length - r].style.width = (eecw - 1) + 'px'; 
							}
						}
						else if(eecsw < eecfw){
							ra = eecfw - eecsw + 1;
							for(r = 1; r < ra; r++){
								eecs[eecs.length - r].style.width = (eecw + 1) + 'px'; 
							}
						}
						var tcw = 0;
						for(var f = 0; f < ll - 1; f++){ tcw += Number(eecs[f].style.width.replace('px','')); }
						eecs[ll-1].style.width = ( econw - ( ecsw + efcsw + (tcw) ) ) + 'px';
					}
					else if(ecol.escol == egml && efcs.length === 0){
						ecol.style.width = econw + 'px';
					}
					else if(ecs.length > 0 && efcs.length === 0 && egnl == egml){
						if(ecsw > econw){
							ra = ecsw - econw + 1;
							for(r = 1; r < ra; r++){
								ecs[ecs.length - r].style.width = ( (ecw - 1) * ecs[ecs.length - r].escol ) + 'px'; 
							}
						}
						else if(ecsw < econw){
							ra = econw - ecsw + 1;
							for(r = 1; r < ra; r++){
								ecs[ecs.length - r].style.width = ( (ecw + 1) * ecs[ecs.length - r].escol ) + 'px'; 
							}
						}
						var tcw = 0;
						for(var f = 0; f < ecs.length - 1; f++){ tcw += Number(ecs[f].style.width.replace('px','')); }
						ec.style.width = ( econw - ( tcw )  ) + 'px';
					}
					else if(egnl < egml && ecolclass.indexOf('final') > -1){
						ecol.style['margin' + ( (econclass.indexOf('inverted') > -1) ? 'Left' : 'Right')] = (econw - ecsw - efcsw) + 'px';
					}
					eg = []; egnl = 0;
				}
				if(ig){eg = [ecol]; egnl = escol;}
			}
		}
		for(i in Elastic.helpers){
			if(Elastic.helpers.hasOwnProperty(i)){
				Elastic.helpers[i](context);
			}
		}
	};

	var Elastic = window.Elastic;

	Elastic.version = '2.0.3';

	Elastic.reset = function Elastic_reset(context){
		var doc = $(document);
		doc.trigger('elastic:beforeReset');
		var i,w,wl,h,hl,p,pl,m,ml;
		h = $.find('.same-height > *, .full-height, .elastic-height', context);
		for(i = 0, hl = h.length; i < hl; i++){h[i].style.height = '';}
		p = $.find('.vertical-center, .center, .bottom', context);
		for(i = 0, pl = p.length; i < pl; i++){p[i].parentNode.style.paddingTop = ''; p[i].parentNode.style.height = '';}
		w = $.find('.column:not(.fixed), .full-width', context);
		for(i = 0, wl = w.length; i < wl; i++){w[i].style.width = '';}
		m = $.find('.column.final', context);
		for(i = 0, ml = m.length; i < ml; i++){m[i].style.marginLeft = ''; m[i].style.marginRight = '';}
		doc.trigger('elastic:reset');
	};

	Elastic.refresh = function Elastic_refresh(context){
		var doc = $(document);
		doc.trigger('elastic:beforeRefresh');
		Elastic.reset(context);
		Elastic(context);
		doc.trigger('elastic:refresh');
	};

	Elastic.configuration = {
		refreshOnResize : true
	};

	Elastic.helpers = {
		'full-width'       : function Elastic_helper_fullWidth(context){
			var i, $el;
			var els = $.find('.full-width', context);
			var elsl = els.length;
			
			for(i = 0; i < elsl; i++){
				$el = $(els[i]);
				$el.width( $el.parent().width() - ( $el.outerWidth(true) - $el.width() ) );
			}
		},
		'same-height'      : function Elastic_helper_sameHeight(context){
			$('.same-height', context).each(function(){
				var columns = $('> *', this);
				var maxHeight = 0;
				columns.each(function(){
					var currentHeight = $(this).outerHeight(true);
					maxHeight = (maxHeight > currentHeight) ? maxHeight : currentHeight;
				}).each(function(){
					$(this).css('height', maxHeight);
				});
			});
		},
		'full-height'      : function Elastic_helper_fullHeight(context){
			$('.full-height', context).each(function(){
				var _this = $(this);
				_this.css('height', $(this.parentNode).height() - ( _this.outerHeight(true) - _this.height() ));
			});
		},
		'elastic-height'   : function Elastic_helper_elasticHeight(context){
			$('.elastic-height', context).each(function(){
				var _this = $(this);
				var h = 0;
				$('> *:not(.elastic-height)', this.parentNode).each(function(){
					h += $(this).outerHeight(true);
				});
				_this.css('height', Math.round(_this.parent().height() - h));
				Elastic.refresh(this);
			});
		},
		'center'           : function Elastic_helper_center(context){
			$('.vertical-center, .center', context).each(function(){
				var parentNode = $(this.parentNode);
				var paddingTop = Math.round( ( parentNode.height() - $(this).outerHeight(true) ) / 2 );
				parentNode.css({
					paddingTop : paddingTop + 'px',
					height     : ( parentNode.css('height') ) ? ( parentNode.outerHeight() - paddingTop ) : ''
				});
			});
		},
		'bottom'          : function Elastic_helper_bottom(context){
			$('.bottom', context).each(function(){
				var parentNode = $(this.parentNode);
				var paddingTop = Math.round( parentNode.height() - $(this).outerHeight(true) );
				parentNode.css({
					paddingTop : paddingTop + 'px',
					height     : ( parentNode.css('height') ) ? ( parentNode.outerHeight() - paddingTop ) : ''
				});
			});
		}
	};
	
	/*
		Elastic Layouts Support
	*/
	$(document).bind('elastic:beforeInitialize', function(){
		var r = /(^|\s+)display\s+([\w\_\-\d]+)(\s+|$)/;
		$('.display').each(function Elastic_layout(){
			r.test(this.className);
			var c = '.position-' + RegExp.$2;
			$(c).removeClass(c).appendTo(this); 
		});
	});
})(jQuery);

// due to a safari 4 final bug, this initialization must be done on window.load event
// definitely a must fix either on elastic or jquery
jQuery(window).bind('load', function(){
	var doc = jQuery(document);
	var iw  = document.body.clientWidth;
	doc.trigger('elastic:beforeInitialize');
	Elastic();
	if(iw != document.body.clientWidth){
		Elastic.refresh();
	}
	jQuery(window).bind('resize',function Elastic_resizeHandler(){
		if(Elastic.configuration.refreshOnResize){
			Elastic.refresh();
		}
	});
	doc.bind('elastic', Elastic.refresh);
	doc.trigger('elastic:initialize');
});

/*
	Elastic CSS Framework
	Released under the MIT, BSD, and GPL Licenses.
	More information http://elasticss.com
	
	Elastic jQueryUIWrapper Module
	Provides
		Convenience initializers for jQuery UI
		Convenience event handlers for jQuery UI
	Requires
		jQueryUI
*/
(function($){
	$(function(){
		$(document).bind('elastic:Initialize', function(){
			$.datepicker.regional['es']
			$('.elastic-ui-datepicker').each(function(){
				$(this).datepicker({
					changeMonth : true,
					changeYear : true, 
					yearRange : '-10:+10'
				});
			});
			
			$('form').submit(function(){
				$('.elastic-ui-datepicker').each(function(e){
					var months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
					var dates = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'];
					var date = $(this).datepicker('getDate');
					if(date){
						$(this).val( [date.getFullYear(), months[date.getMonth()], dates[date.getDate()] ].join('-') );
					}
				});
			});
		});
	});
})(jQuery);
