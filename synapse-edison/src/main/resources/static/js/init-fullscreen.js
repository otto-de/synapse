//Toggle fullscreen
var toggleFullScreen = function (e) {
    e.preventDefault();
    var $this = $(this);

    if ($this.children('span').hasClass('glyphicon-resize-full')) {
        $this.children('span').removeClass('glyphicon-resize-full');
        $this.children('span').addClass('glyphicon-resize-small');
    }
    else if ($this.children('span').hasClass('glyphicon-resize-small')) {
        $this.children('span').removeClass('glyphicon-resize-small');
        $this.children('span').addClass('glyphicon-resize-full');
    }
    var panel = $(this).closest('.panel');
    panel.toggleClass('panel-fullscreen');
    panel.children('.panel-collapse').collapse('show');
};
$(document).ready(function () {
    $(".toggle-fullscreen").each(function (index) {
        $(this).click(toggleFullScreen)
    });
});